package pkg

import (
	"context"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// NewConnectMessage denotes a new connect control message. The message
	// payload is binary data.
	ConnectMessage = 0

	// TextMessage denotes a text data message. The text message payload is
	// interpreted as UTF-8 encoded text data.
	TextMessage = 1

	// BinaryMessage denotes a binary data message.
	BinaryMessage = 2

	// CloseMessage denotes a close control message. The optional message
	// payload contains a numeric code and text. Use the FormatCloseMessage
	// function to format a close message payload.
	CloseMessage = 8

	// PingMessage denotes a ping control message. The optional message payload
	// is UTF-8 encoded text.
	PingMessage = 9

	// PongMessage denotes a pong control message. The optional message payload
	// is UTF-8 encoded text.
	PongMessage = 10

	CloseSessionMessage = 11
)

const (
	ProtocolSSH       = "ssh"
	ProtocolHttp      = "http"
	ProtocolWebsocket = "ws"
	//containers
	ProtocolExec = "exec"
	ProtocolLogs = "logs"
)

type SessionStatus string

const (
	SessionConnecting       SessionStatus = "connecting"
	SessionConnected        SessionStatus = "connected"
	SessionUpstreamClosed   SessionStatus = "upstreamClosed"
	SessionDownstreamClosed SessionStatus = "downstreamClosed"
	SessionClosed           SessionStatus = "Closed"
	SessionConnectFailed    SessionStatus = "connectFailed"
	SessionProcessFailed    SessionStatus = "processFailed"
)

const (
	// Time allowed to write a message to the peer.
	WriteWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	PongWait = 5 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	PingPeriod = (PongWait * 9) / 10

	// Maximum message size allowed from peer.
	MaxMessageSize = 512

	HttpRequestTimeout = 20 * time.Second
)

const (
	ServerInternalError string = "Service internal error"
)

type TunnelMessage struct {
	SessionID   string       `json:"session,omitempty"` //仅仅ws连接需要提供session字段
	Protocol    string       `json:"protocol"`          //必填 http,ws,ssh,exec,logs
	MessageType int          `json:"action"`            //可以隐藏，通过websocket原生的消息类型区分
	Payload     []byte       `json:"payload,omitempty"`
	Ws          *Ws          `json:"ws,omitempty"`
	SSH         *SSH         `json:"ssh,omitempty"`
	Logs        *Container   `json:"logs,omitempty"`
	Exec        *Container   `json:"exec,omitempty"`
	HttpRequest *HttpRequest `json:"http,omitempty"`
}

type Ws struct {
	Host string `json:"host,omitempty"`
	Path string `json:"path,omitempty"`
}

type SSH struct {
	IP       string `json:"ip,omitempty"` //127.0.0.1
	UserName string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

type Container struct {
	NameSpace string `json:"namespace,omitempty"`
	Pod       string `json:"pod,omitempty"`
	Container string `json:"container,omitempty"`
}

type VM struct {
	VMName   string `json:"vmName,omitempty"`
	UserName string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

type HttpRequest struct {
	HttpRequest string `json:"HttpRequest"`
	URL         string `json:"url"`
}

type HttpResponse struct {
	Status      string              `json:"status,omitempty"`     // e.g. "200 OK"
	StatusCode  int                 `json:"statusCode,omitempty"` // e.g. 200
	Proto       string              `json:"proto,omitempty"`      // e.g. "HTTP/1.0"
	Header      map[string][]string `json:"header"`
	Body        []byte              `json:"body,omitempty"`
	ContentType string              `json:"contentType,omitempty"`
}
type Client struct {
	Socket *websocket.Conn
	Mu     sync.Mutex
	//	Cancel context.CancelFunc
	Send     chan *TunnelMessage
	ClientID string //downstream->clientID  upstream->sessionid
	Cancel   context.CancelFunc
	Context  context.Context
}
type Session struct {
	SessionID  string
	UpAgent    interface{}
	DownAgent  interface{}
	Protocol   string
	Annotation string
	Cancel     context.CancelFunc
	Context    context.Context
	Status     SessionStatus
	Mu         sync.Mutex
}

func (client *Client) WsLockWriteMessage(messageType int, data []byte) error {
	client.Mu.Lock()
	defer client.Mu.Unlock()
	client.Socket.SetWriteDeadline(time.Now().Add(WriteWait))
	return client.Socket.WriteMessage(messageType, data)
}
