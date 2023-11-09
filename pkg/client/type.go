package client

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"net/http"
	"net/url"
	tunnelPkg "swarm-tunnel/pkg"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Client tunnelPkg.Client

type Session tunnelPkg.Session

type SshClient struct{}
type ExecClient struct{}

type Hub struct {
	// Register requests from the clients.
	// Unregister requests from clients.
	SessionRegister   chan *Session
	SessionUnregister chan *Session
	c                 *Client
	Sessions          sync.Map
}

type TunnelAgent struct {
	hub *Hub
}

// type Session struct {
// 	SessionID  string
// 	UpAgent    interface{}
// 	DownAgent  downstream.DownStream
// 	Protocol   string
// 	Annotation string
// 	Cancel     context.CancelFunc
// }

type upAgent struct {
	//out io.Writer
	// in  io.Reader
	out *io.PipeWriter
	in  *io.PipeReader
}

func (session *Session) close() {
	session.UpAgent.(*upAgent).in.Close()
	session.UpAgent.(*upAgent).out.Close()
	session.DownAgent.(*upAgent).in.Close()
	session.DownAgent.(*upAgent).out.Close()
	session.Cancel()
}

func (session *Session) upAgent() *upAgent {
	return session.UpAgent.(*upAgent)
}

func (session *Session) downAgent() *upAgent {
	return session.DownAgent.(*upAgent)
}

// type DownStream interface {
// 	Connector() error
// 	Processor()
// }

func (client *Client) WsLockWriteMessage(messageType int, data []byte) error {
	client.Mu.Lock()
	defer client.Mu.Unlock()
	client.Socket.SetWriteDeadline(time.Now().Add(tunnelPkg.WriteWait))
	return client.Socket.WriteMessage(messageType, data)
}

func NewWsConnect(proxyServer, path string) (*websocket.Conn, error) {
	u := url.URL{Scheme: "ws", Host: proxyServer, Path: path}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	return c, err
}

func NewProxyClient(proxyServer, path string) (*Client, error) {
	c, err := NewWsConnect(proxyServer, path)
	if err != nil {
		return nil, err
	}
	return &Client{Socket: c, Send: make(chan *tunnelPkg.TunnelMessage), Mu: sync.Mutex{}}, nil
}

// deserialize request
func wsToLocalRequest(websocketReq *tunnelPkg.HttpRequest) (*http.Request, error) {
	r := bufio.NewReader(bytes.NewReader([]byte(websocketReq.HttpRequest)))
	localRequest, err := http.ReadRequest(r)
	if err != nil {
		log.Println("deserialize request error", err)
		return localRequest, err
	}

	localRequest.RequestURI = ""
	u, err := url.Parse(websocketReq.URL)
	if err != nil {
		log.Println("parse url error", err)
		return nil, err
	}
	localRequest.URL = u
	localRequest.URL.Scheme = u.Scheme
	localRequest.URL.Host = u.Host

	log.Println("localRequest.URL", localRequest.URL)
	log.Println("localRequest.URL.Host", localRequest.URL.Host)
	return localRequest, nil
}

func localResponseToWebSocketResponse(resp *http.Response) (*tunnelPkg.HttpResponse, error) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println("read local response error ", err)
		return nil, err
	}
	resp.Body.Close()
	wsRes := &tunnelPkg.HttpResponse{Status: resp.Status, StatusCode: resp.StatusCode,
		Proto: resp.Proto, Header: resp.Header, Body: body, ContentType: resp.Header.Get("Content-Type")}
	return wsRes, nil
}
