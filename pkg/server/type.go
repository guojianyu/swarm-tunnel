package server

import (
	"bytes"
	"io"
	"net/http"
	tunnelPkg "swarm-tunnel/pkg"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
)

//上层流断线，代理服务向下层流客户端发送关闭session
//下层流断线，代理服务向上层流客户端发送连接关闭。
//没有断线的异常情况，
//代理服务：定时发送session心跳，如果没有心跳则断开上层流的连接
//客户端：定时向代理服务发送pong信号，没有响应则关闭session。

type Client tunnelPkg.Client
type Session tunnelPkg.Session

// type ClientMap tunnelPkg.ClientMap
// type SessionMap tunnelPkg.SessionMap

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

func (server *TunnelServer) SessionIDGenarator() string {
	return uuid.NewV4().String()

}

type TunnelServer struct {
	hub *Hub
}

type Hub struct {
	UpStreamClients   sync.Map
	DownStreamClients sync.Map
	// Register requests from the clients.
	// Unregister requests from clients.
	UpStreamRegister     chan *Client
	UpStreamUnregister   chan *Client
	DownStreamRegister   chan *Client
	DownStreamUnregister chan *Client
	SessionRegister      chan *Session
	SessionUnregister    chan *Session
	Sessions             sync.Map
}

func (client *Client) WsLockWriteMessage(messageType int, data []byte) error {
	client.Mu.Lock()
	defer client.Mu.Unlock()
	client.Socket.SetWriteDeadline(time.Now().Add(tunnelPkg.WriteWait))
	return client.Socket.WriteMessage(messageType, data)
}

func (client *Client) Close() {
	if client != nil {
		client.Socket.Close()
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
