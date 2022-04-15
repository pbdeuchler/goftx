package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/valyala/fastjson"
	"nhooyr.io/websocket"
)

const (
	wsUrlFormat = "wss://ftx.%s/ws/"

	writeWait                = time.Second * 10
	defaultReconnectCount    = int(10)
	defaultReconnectInterval = time.Second
	defaultWSTimeout         = time.Second * 60
	defaultTimeDiff          = time.Second * 0
)

type subscription struct {
	Op      string `json:"op"`
	Channel string `json:"channel"`
	Market  string `json:"market"`
}

// type OrderbookHandler func(market string, timestamp float64, checksum int64, side, level, volume string)
type OrderbookHandler func(string, float64, int64, string, string, string)

type Stream struct {
	apiKey     string
	secret     string
	subAccount string
	mu         *sync.Mutex
	url        string
	// dialer            *websocket.Dialer
	reconnectCount    int
	reconnectInterval time.Duration
	timeout           time.Duration
	debug             bool
	serverTimeDiff    time.Duration
	authorized        bool
	log               *logrus.Logger

	conn *websocket.Conn

	closeWG *sync.WaitGroup
	ctx     context.Context
	// closeChan chan interface{}

	subscriptions []*subscription
	opts          *websocket.DialOptions

	orderbookHandler OrderbookHandler
}

func NewStream(ctx context.Context, apiKey, secret, subAccount, url string,
	reconnectCount int,
	reconnectInterval, streamTimeout, serverTimeDiff time.Duration,
	orderbookHandler OrderbookHandler,
	debug bool, log *logrus.Logger) (*Stream, error) {

	var zeroDuration time.Duration
	if streamTimeout == zeroDuration {
		streamTimeout = defaultWSTimeout
	}

	s := &Stream{
		apiKey:            apiKey,
		secret:            secret,
		subAccount:        subAccount,
		mu:                &sync.Mutex{},
		url:               url,
		reconnectCount:    reconnectCount,
		reconnectInterval: reconnectInterval,
		timeout:           streamTimeout,
		debug:             debug,
		log:               log,
		serverTimeDiff:    serverTimeDiff,
		authorized:        false,
		closeWG:           &sync.WaitGroup{},
		ctx:               ctx,

		orderbookHandler: orderbookHandler,

		// closeChan:         make(chan interface{}, 1),
	}
	var err error
	// TODO: add configurable options
	// this is because ftx has a shitty websocket imp lol
	s.opts = &websocket.DialOptions{CompressionMode: websocket.CompressionContextTakeover}
	s.conn, _, err = s.connect(nil)
	if err != nil {
		return nil, err
	}
	return s, err
}

func (s *Stream) Wait() {
	s.closeWG.Wait()
}

func (s *Stream) connect(overrideOpts *websocket.DialOptions) (*websocket.Conn, *http.Response, error) {
	opts := s.opts
	if overrideOpts != nil {
		opts = overrideOpts
	}
	conn, resp, err := websocket.Dial(s.ctx, s.url, opts)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	s.closeWG.Add(1)
	s.printf("connected to %s", s.url)

	return conn, resp, err
}

// func (s *Stream) reconnect(opts *websocket.DialOptions) (*websocket.Conn, *http.Response, error) {
// 	started := time.Now()

// 	for i := 1; i < s.wsReconnectionCount; i++ {
// 		conn, err := s.connect(requests...)
// 		if err == nil {
// 			return conn, nil
// 		}

// 		timeout := time.Duration(int64(math.Pow(2, float64(i)))) * time.Second

// 		select {
// 		case <-time.After(timeout):
// 			conn, err := s.connect(requests...)
// 			if err != nil {
// 				continue
// 			}

// 			return conn, nil
// 		case <-ctx.Done():
// 			return nil, ctx.Err()
// 		}
// 	}

// 	return nil, errors.Errorf("reconnection failed after %v", time.Since(started))
// }

func (s *Stream) close(err error) {
	s.printf("closing")
	if s.conn != nil {
		if err != nil {
			s.printf("with error: %s", err.Error())
			s.conn.Close(websocket.StatusNormalClosure, "")
			// s.conn.Close(websocket.StatusInternalError, err.Error())
		} else {
			s.conn.Close(websocket.StatusNormalClosure, "")
		}
	}
}

func (s *Stream) printf(msg string, v ...interface{}) {
	if !s.debug {
		return
	}
	if len(v) > 0 {
		s.log.Info(fmt.Sprintf(msg, v...))
	} else {
		s.log.Info(msg)
	}
}

func (s *Stream) Read() (err error) {
	var wasClosed bool

	defer func() {
		if err != nil {
			if wasClosed {
				s.printf("connection was closed, exiting")
			} else {
				s.printf("connection errored, exiting")
				s.close(err)
			}
		}
		s.closeWG.Done()
	}()
	var msgBytes []byte
	var p fastjson.Parser
	// so we don't have to allocate structs
	// this might be a really stupid optimization
	var level, volume string
	var levelBuffer, volumeBuffer []byte

	var v, data *fastjson.Value
	var channel, msgType []byte
	var bidVals, askVals []*fastjson.Value
	var bidVal, askVal *fastjson.Value

	pingTimer := time.After((s.timeout * 9) / 10)
	// s.log.Info("WTF6")
	for {
		select {
		case <-s.ctx.Done():
			s.close(nil)
			return nil
		case <-pingTimer:
			s.printf("PING")
			go func() {
				pingErr := s.conn.Ping(s.ctx)
				if pingErr != nil {
					// we consider this fatal (maybe?)
					s.log.Fatal(pingErr.Error())
				}
				// reset timer
				pingTimer = time.After((s.timeout * 9) / 10)
			}()
		default:
			_, r, err := s.conn.Reader(s.ctx)
			if err != nil {
				if int(websocket.CloseStatus(err)) != -1 {
					wasClosed = true
					return err
				}
				if strings.Contains(err.Error(), "reset by peer") { // those mfs
					reconnectCount := 3
					for (err != nil) && (reconnectCount > 0) {
						if reconnectCount != 3 {
							<-time.After(time.Second * 3)
						}
						s.printf("Reconnecting due to peer disconnect")
						s.conn, _, err = s.connect(nil)
						if (err != nil) && (reconnectCount == 1) {
							return fmt.Errorf("Reconnect error after peer disconnect: %w", err)
						}
						s.printf("Resubscribing")
						for _, sub := range s.subscriptions {
							err = s.subscribe(s.ctx, sub)
							if (err != nil) && (reconnectCount == 1) {
								return fmt.Errorf("Resub error after peer disconnect: %w", err)
							}
						}
						reconnectCount--
					}
				}
				if err != nil {
					return err
				}
				continue
			}

			// s.printf("typ: %s", typ)

			msgBytes, err = ioutil.ReadAll(r)
			if err != nil {
				return err
			}

			// s.printf("msg: %s", string(msgBytes))

			v, err = p.Parse(string(msgBytes))
			if err != nil {
				return err
			}

			// After this we can probably abstract this away to
			// exchange specific adapters

			msgType = v.GetStringBytes("type")

			switch string(msgType) {
			// Don't care about these
			case "subscribed":
			case "unsubscribed":
			case "info":
				if v.GetInt("code") == 20001 {
					s.printf("20001 code")
					s.printf("Reconnecting")
					s.conn, _, err = s.connect(nil)
					if err != nil {
						return fmt.Errorf("Reconnect error after 20001 code: %w", err)
					}
					s.printf("Resubscribing")
					for _, sub := range s.subscriptions {
						err = s.subscribe(s.ctx, sub)
						if err != nil {
							return fmt.Errorf("Resub error after 20001 code: %w", err)
						}
					}
					continue
				} else {
					continue // TODO: fix
				}
			case "error": // TODO: fix lol
				return fmt.Errorf("Error msg received from server: code=%d, msg=%s", string(v.GetInt("code")), string(v.GetStringBytes("msg")))
			case "partial":
				fallthrough
			case "update":
				channel = v.GetStringBytes("channel")
				switch string(channel) {
				case "orderbook":
					data = v.Get("data")
					bidVals = data.GetArray("bids")
					// move handler call within these loops
					for _, bidVal = range bidVals {
						level = string(bidVal.Get("0").MarshalTo(levelBuffer[:0]))
						volume = string(bidVal.Get("1").MarshalTo(volumeBuffer[:0]))
						if s.orderbookHandler != nil {
							s.orderbookHandler(string(v.GetStringBytes("market")),
								data.GetFloat64("time"),
								data.GetInt64("checksum"),
								"bid", level, volume)
						} else {
							s.printf("market: %s, time: %f, checksum: %d, side: %s, level: %d, volume: %f", v.Get("market"), data.GetFloat64("time"), data.GetInt("checksum"), "bid", level, volume)
						}
					}
					askVals = data.GetArray("asks")
					for _, askVal = range askVals {
						level = string(askVal.Get("0").MarshalTo(levelBuffer[:0]))
						volume = string(askVal.Get("1").MarshalTo(volumeBuffer[:0]))
						if s.orderbookHandler != nil {
							s.orderbookHandler(string(v.GetStringBytes("market")),
								data.GetFloat64("time"),
								data.GetInt64("checksum"),
								"ask", level, volume)
						} else {
							s.printf("market: %s, time: %f, checksum: %d, side: %s, level: %d, volume: %f", v.Get("market"), data.GetFloat64("time"), data.GetInt("checksum"), "ask", level, volume)
						}
					}
				case "trades":
					return fmt.Errorf("Not implemented: %s", string(msgBytes))
				case "ticker":
					return fmt.Errorf("Not implemented: %s", string(msgBytes))
				default:
					return fmt.Errorf("Unreachable. Invalid channel type: %s", channel)
				}
			default:
				return fmt.Errorf("Unreachable. Invalid message type: %s", string(msgType))
			}
		}
	}
}

func (s *Stream) subscribe(ctx context.Context, sub *subscription) error {
	if sub.Market == "" {
		return fmt.Errorf("Market required")
	}

	if sub.Channel == "" {
		return fmt.Errorf("Channel required")
	}

	sub.Op = "subscribe"
	jsonBytes, err := json.Marshal(sub)
	if err != nil {
		return err
	}
	return s.conn.Write(ctx, websocket.MessageText, jsonBytes)
}

func (s *Stream) SubscribeToOrderBooks(ctx context.Context, markets ...string) error {
	if len(markets) == 0 {
		return errors.New("no markets provided")
	}
	for _, market := range markets {
		sub := &subscription{Market: market, Channel: "orderbook"}
		s.subscriptions = append(s.subscriptions, sub)
		err := s.subscribe(ctx, sub)
		if err != nil {
			return err
		}
	}
	return nil
}
