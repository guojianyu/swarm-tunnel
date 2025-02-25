package client

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
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

func (s *Session) setStatus(status tunnelPkg.SessionStatus) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if s.Status == tunnelPkg.SessionClosed || s.Status == tunnelPkg.SessionDownstreamClosed || s.Status == tunnelPkg.SessionUpstreamClosed {
		return
	}
	s.Status = status
}

func (s *Session) setAnnotaion(annotation string) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if s.Status == tunnelPkg.SessionClosed {
		return
	}
	s.Annotation = annotation
}

type Hub struct {
	// Register requests from the clients.
	// Unregister requests from clients.
	SessionRegister   chan *Session
	SessionUnregister chan *Session
	c                 *Client
	Sessions          sync.Map
}

type TunnelAgent struct {
	hub       *Hub
	webserver *tunnelPkg.WebServer
	ClientID  string
}

func (ta *TunnelAgent) WithTLS(cafile, certFile, keyFile string) {
	ta.webserver.EnableTLS = true
	ta.webserver.CaFile = cafile
	ta.webserver.CertFile = certFile
	ta.webserver.KeyFile = keyFile
}
func NewHub() *Hub {
	return &Hub{
		SessionRegister:   make(chan *Session),
		SessionUnregister: make(chan *Session),
		Sessions:          sync.Map{},
	}
}

func NewTunnelAgent(addr, clientID string) *TunnelAgent {
	return &TunnelAgent{
		hub: NewHub(),
		webserver: &tunnelPkg.WebServer{
			Addr: addr,
		},
		ClientID: clientID,
	}
}

type pump struct {
	in  *io.PipeWriter
	out *io.PipeReader
}

func (session *Session) close() {
	session.UpAgent.(*pump).in.Close()
	session.UpAgent.(*pump).out.Close()
	session.DownAgent.(*pump).in.Close()
	session.DownAgent.(*pump).out.Close()
	session.Cancel()
}

func (session *Session) upAgent() *pump {
	return session.UpAgent.(*pump)
}

func (session *Session) downAgent() *pump {
	return session.DownAgent.(*pump)
}

func (client *Client) WsLockWriteMessage(messageType int, data []byte) error {
	client.Mu.Lock()
	defer client.Mu.Unlock()
	client.Socket.SetWriteDeadline(time.Now().Add(tunnelPkg.WriteWait))
	return client.Socket.WriteMessage(messageType, data)
}

func (ta *TunnelAgent) close() {
	ta.hub.c.Socket.Close()
}

func (ta *TunnelAgent) newWebSocketClient() error {
	u, err := url.Parse(ta.webserver.Addr)
	if err != nil {
		return fmt.Errorf("invalid URL: %v", err)
	}
	query := u.Query()
	query.Set("clientid", ta.ClientID)
	u.RawQuery = query.Encode()
	dialer := websocket.Dialer{}
	if ta.webserver.EnableTLS {
		caCert, err := ioutil.ReadFile(ta.webserver.CaFile)
		if err != nil {
			return fmt.Errorf("failed to read CA cert: %v", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		// load client certificate and key
		clientCert, err := tls.LoadX509KeyPair(ta.webserver.CertFile, ta.webserver.KeyFile)
		if err != nil {
			return fmt.Errorf("failed to load client cert/key: %v", err)
		}

		// configure TLS
		tlsConfig := &tls.Config{
			RootCAs:      caCertPool,                    // CA certificate
			Certificates: []tls.Certificate{clientCert}, // client certificate
		}
		dialer.TLSClientConfig = tlsConfig
	}
	conn, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to connect: %v", err)
	}
	ta.hub.c = &Client{Socket: conn, Send: make(chan *tunnelPkg.TunnelMessage), Mu: sync.Mutex{}}
	return nil
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
