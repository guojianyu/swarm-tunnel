package server

import (
	"bytes"
	"io"
	"net/http"
	"sync"
	"time"

	tunnelPkg "github.com/guojianyu/swarm-tunnel/pkg"

	"k8s.io/klog"

	"github.com/gorilla/mux"
	uuid "github.com/satori/go.uuid"
)

type Client tunnelPkg.Client
type Session tunnelPkg.Session

type Hub struct {
	DownStreamClients sync.Map
	// Register requests from the clients.
	// Unregister requests from clients.
	DownStreamRegister        chan *Client
	DownStreamUnregister      chan *Client
	SessionRegister           chan *Session
	SessionUnregister         chan *Session
	Sessions                  sync.Map
	registerClientCallback    func(string)
	unregisterClientCallback  func(string)
	registerSessionCallback   func(string)
	unregisterSessionCallback func(string)
}
type TunnelServer struct {
	hub       *Hub
	webserver *tunnelPkg.WebServer
	Router    *mux.Router
}

func (ts *TunnelServer) WithTLS(cafile, certFile, keyFile string) {
	ts.webserver.EnableTLS = true
	ts.webserver.CaFile = cafile
	ts.webserver.CertFile = certFile
	ts.webserver.KeyFile = keyFile
}

func (ts *TunnelServer) SetRegisterClientCallback(cb func(string)) {
	ts.hub.registerClientCallback = cb
}

func (ts *TunnelServer) SetUnregisterClientCallback(cb func(string)) {
	ts.hub.unregisterClientCallback = cb
}
func (ts *TunnelServer) SetRegisterSessionCallback(cb func(string)) {
	ts.hub.registerSessionCallback = cb
}

func (ts *TunnelServer) SetUnregisterSessionCallback(cb func(string)) {
	ts.hub.unregisterSessionCallback = cb
}

func NewHub() *Hub {
	return &Hub{
		DownStreamRegister:   make(chan *Client),
		DownStreamUnregister: make(chan *Client),
		SessionRegister:      make(chan *Session),
		SessionUnregister:    make(chan *Session),
		DownStreamClients:    sync.Map{},
		Sessions:             sync.Map{},
	}
}

func NewTunnelServer(addr string) *TunnelServer {
	return &TunnelServer{
		hub: NewHub(),
		webserver: &tunnelPkg.WebServer{
			Addr: addr,
		},
		Router: mux.NewRouter(),
	}
}

func (s *Session) SetStatus(status tunnelPkg.SessionStatus) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if s.Status == tunnelPkg.SessionClosed || s.Status == tunnelPkg.SessionDownstreamClosed || s.Status == tunnelPkg.SessionUpstreamClosed {
		return
	}
	s.Status = status
}

func (s *Session) GetStatus() tunnelPkg.SessionStatus {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	return s.Status
}

func (s *Session) SetAnnotaion(annotation string) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if s.Status == tunnelPkg.SessionClosed {
		return
	}
	s.Annotation = annotation
}

func (s *Session) GetAnnotaion(annotation string) string {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	return s.Annotation
}

func (server *TunnelServer) sessionIDGenarator() string {
	return uuid.NewV4().String()

}

func (client *Client) WsLockWriteMessage(messageType int, data []byte) error {
	client.Mu.Lock()
	defer client.Mu.Unlock()
	client.Socket.SetWriteDeadline(time.Now().Add(tunnelPkg.WriteWait))
	return client.Socket.WriteMessage(messageType, data)
}

func (client *Client) close() {
	client.Mu.Lock()
	defer client.Mu.Unlock()
	if !client.IsClosed {
		close(client.Send)
		client.IsClosed = true
	}
	if client.Socket != nil {
		client.Socket.Close()
	}

}

// close upper stream if session closure
func (s *Session) close() {
	klog.V(2).Infof("session:%v is closed", s.SessionID)
	s.upAgent().Mu.Lock()
	defer s.upAgent().Mu.Unlock()
	if !s.upAgent().IsClosed {
		close(s.upAgent().Send)
		s.upAgent().IsClosed = true
	}
	if s.upAgent().Socket != nil {
		s.upAgent().Socket.Close()
	}

}

func converseClient(c interface{}) (client *Client, ok bool) {
	client, ok = (c).(*Client)
	return
}

func converseSession(s interface{}) (session *Session, ok bool) {
	session, ok = (s).(*Session)
	return
}
func (session *Session) upAgent() *Client {
	return session.UpAgent.(*Client)
}
func (session *Session) downAgent() *Client {
	return session.DownAgent.(*Client)
}

func httpRequestToWebSocketRequest(r *http.Request, url string) (httpreq *tunnelPkg.HttpRequest, err error) {

	var b = &bytes.Buffer{}            // holds serialized representation
	if err := r.Write(b); err != nil { // serialize request to HTTP/1.1 wire format
		return nil, err
	}
	httpreq = &tunnelPkg.HttpRequest{HttpRequest: b.String(), URL: url}
	return httpreq, nil
}

func wsResultToHttpResponse(w http.ResponseWriter, wsRes tunnelPkg.HttpResponse) {
	for k, vv := range wsRes.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(wsRes.StatusCode)
	w.Header().Set("Content-Type", wsRes.ContentType)
	io.Copy(w, bytes.NewReader(wsRes.Body))
}

func (hub *Hub) ByIdGetSession(sessionID string) (*Session, bool) {
	if s, ok := hub.Sessions.Load(sessionID); ok {
		session, ok := converseSession(s)
		return session, ok
	}
	return nil, false
}
