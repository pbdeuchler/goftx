package v2

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	apiUrlFormat = "https://ftx.%s/api"
	apiOtcUrl    = "https://otc.ftx.com/api"

	keyHeaderFormat        = "FTX%s-KEY"
	signHeaderFormat       = "FTX%s-SIGN"
	tsHeaderFormat         = "FTX%s-TS"
	subAccountHeaderFormat = "FTX%s-SUBACCOUNT"
)

// type Option func(c Optionable)
type Option func(c *Client)

// type Optionable interface {
// 	SetHTTPClient(*http.Client)
// 	SetIsUS(bool)
// 	SetAuth(string, string, string)

func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) {
		c.client = client
	}
}

func WithFTXUS() Option {
	return func(c *Client) {
		c.isFtxUS = true
	}
}

func WithAuth(key, secret string, subAccount string) Option {
	return func(c *Client) {
		c.apiKey = key
		c.secret = secret

		c.subAccount = subAccount
	}
}

func WithDebug(log *logrus.Logger) Option {
	return func(c *Client) {
		c.debug = true
		c.log = log
	}
}

type Client struct {
	client         *http.Client
	apiKey         string
	secret         string
	subAccount     string
	serverTimeDiff time.Duration
	isFtxUS        bool
	apiURL         string
	debug          bool
	log            *logrus.Logger
	// SubAccounts
	// Markets
	// Account
	// Orders
	// Fills
	// Converts
	// Futures
	// SpotMargin
	// Wallet
}

func New(opts ...Option) *Client {
	client := &Client{
		client: http.DefaultClient,
	}

	for _, opt := range opts {
		opt(client)
	}

	if client.log == nil {
		client.log = logrus.New()
	}

	client.apiURL = fmt.Sprintf(apiUrlFormat, client.Domain())

	// client.SubAccounts = SubAccounts{client: client}
	// client.Markets = Markets{client: client}
	// client.Account = Account{client: client}
	// client.Orders = Orders{client: client}
	// client.Fills = Fills{client: client}
	// client.Converts = Converts{client: client}
	// client.Futures = Futures{client: client}
	// client.SpotMargin = SpotMargin{client: client}
	// client.Wallet = Wallet{client: client}

	return client
}

func (client *Client) Domain() (domain string) {
	domain = "com"
	if client.isFtxUS {
		domain = "us"
	}
	return
}

func (client *Client) Stream(ctx context.Context, orderbookHandler OrderbookHandler) (*Stream, error) {
	// if client.stream == nil {
	return NewStream(ctx, client.apiKey, client.secret, client.subAccount,
		fmt.Sprintf(wsUrlFormat, client.Domain()),
		defaultReconnectCount, defaultReconnectInterval, defaultWSTimeout, defaultTimeDiff,
		orderbookHandler,
		client.debug, client.log)
	// }
	// return client.stream
}
