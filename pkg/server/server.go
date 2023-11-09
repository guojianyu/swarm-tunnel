package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	tunnelPkg "swarm-tunnel/pkg"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"k8s.io/klog"
)

func (c *Client) downStreamReadPump(hub *Hub) {
	c.Socket.SetReadDeadline(time.Now().Add(tunnelPkg.PongWait))
	c.Socket.SetPongHandler(func(string) error { c.Socket.SetReadDeadline(time.Now().Add(tunnelPkg.PongWait)); return nil })
	for {
		msgtype, message, err := c.Socket.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				klog.V(1).Infof("Failed to read data from downstream,%v.", err)
			}
			klog.V(1).Infof("Failed to read data from downstream,%v.", err)
			break
		}

		switch msgtype {
		case tunnelPkg.PingMessage:

		default:
			tunnelMessage := new(tunnelPkg.TunnelMessage)
			if err := json.Unmarshal(message, tunnelMessage); err != nil {
				klog.Warning("(%v)unmarshal error:%v", message, err)
				continue
			}
			sessionID := tunnelMessage.SessionID
			if session, ok := hub.ByIdGetSession(sessionID); ok {
				klog.V(2).Infof("Session[%s] from agent[%s] received message: %v", sessionID, c.ClientID, tunnelMessage)
				session.upAgent().Send <- tunnelMessage
			} else {
				// if tunnelMessage.Protocol == tunnelPkg.ProtocolHttp {
				// 	continue
				// }
				//The up stream session is closed,notifies down stream to close the session
				tunnelMessage.MessageType = tunnelPkg.CloseMessage
				msg, err := json.Marshal(tunnelMessage)
				if err != nil {
					klog.Warningf("(%v)Marshal error: %v", tunnelMessage, err)
					continue
				}
				c.WsLockWriteMessage(tunnelPkg.TextMessage, msg)
			}

		}
	}
}

func (c *Client) downStreamPing(hub *Hub) {
	defer func() {
		hub.DownStreamUnregister <- c
	}()
	ticker := time.NewTicker(tunnelPkg.PingPeriod)
	for {
		select {
		case <-ticker.C:
			if err := c.WsLockWriteMessage(tunnelPkg.PingMessage, []byte{}); err != nil {
				klog.V(1).Infof("Failed to write data to agent[%s] ,%v.", c.ClientID, err)
				return
			}
		}
	}
}

func (c *Client) downStreamWritePump(hub *Hub) {

	for {
		select {
		case tunnelMessage, ok := <-c.Send:
			if !ok {
				klog.V(1).Infof("downStreamWritePump,%v.", ok)
				return
			}
			//klog.V(2).Infof("Session[%s] to agent[%s] send message: %v", tunnelMessage.SessionID, c.ClientID, tunnelMessage)
			msg, err := json.Marshal(tunnelMessage)
			if err != nil {
				klog.Warning("(%v)Marshal error: %v", tunnelMessage, err)
				continue
			}
			if err := c.WsLockWriteMessage(tunnelPkg.TextMessage, msg); err != nil {
				klog.V(1).Infof("Failed to write data to agent[%s] ,%v.", c.ClientID, err)
				//notice hub unregister session if downstream write failed
				if session, ok := hub.ByIdGetSession(tunnelMessage.SessionID); ok {
					session.SetStatus(tunnelPkg.SessionDownstreamClosed)
					hub.SessionUnregister <- session
				}
				return
			}
		}

	}
}

func (s *Session) upStreamReadPump(hub *Hub, ctx context.Context) {
	s.upAgent().Socket.SetReadDeadline(time.Now().Add(tunnelPkg.PongWait))
	s.upAgent().Socket.SetPongHandler(func(string) error { s.upAgent().Socket.SetReadDeadline(time.Now().Add(tunnelPkg.PongWait)); return nil })
	go func() {
		defer func() {
			klog.V(2).Infof("exit upStreamReadPump")
			if session, ok := hub.ByIdGetSession(s.SessionID); ok {
				hub.SessionUnregister <- session
			}
		}()
		for {
			msgtype, message, err := s.upAgent().Socket.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					klog.Error("error: ", err)
				}
				s.SetStatus(tunnelPkg.SessionUpstreamClosed)
				return
			}
			if s.GetStatus() != tunnelPkg.SessionConnected {
				s.upAgent().WsLockWriteMessage(tunnelPkg.TextMessage, []byte("Wait for the session connection to complete"))
				continue
			}
			tunnelMessage := new(tunnelPkg.TunnelMessage)
			tunnelMessage.MessageType = msgtype
			tunnelMessage.Payload = []byte(message)
			tunnelMessage.SessionID = s.SessionID
			s.downAgent().Send <- tunnelMessage

		}
	}()

	select {
	case <-ctx.Done():
		return
	case <-s.downAgent().Context.Done():
		return
	}
}

func (s *Session) upStreamWritePump(hub *Hub, ctx context.Context) {
	defer func() {
		klog.V(2).Infof("exit upStreamWritePump")
		if session, ok := hub.ByIdGetSession(s.SessionID); ok {
			hub.SessionUnregister <- session
		}
	}()
	ticker := time.NewTicker(tunnelPkg.PingPeriod)
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.downAgent().Context.Done():
			return
		case tunnelMessage, ok := <-s.upAgent().Send:
			if !ok {
				// The hub closed the channel.
				//c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			klog.V(2).Infof("Session[%s] from agent[%s] received message: %v", tunnelMessage.SessionID, s.downAgent().ClientID, tunnelMessage)
			//连接信息需要处理，如果客户端建立连接失败应该主动断开session
			switch tunnelMessage.MessageType {
			case tunnelPkg.ConnectMessage:
				if s.GetStatus() == tunnelPkg.SessionConnecting {
					s.SetStatus(tunnelPkg.SessionConnected)
					//Notifies the hub that the session is connected
					hub.SessionRegister <- s
				}
				continue
			case tunnelPkg.CloseMessage:
				s.SetStatus(tunnelPkg.SessionDownstreamClosed)
				return
			case tunnelPkg.PingMessage:
				continue
			}
			err := s.upAgent().WsLockWriteMessage(tunnelPkg.TextMessage, tunnelMessage.Payload)
			if err != nil {
				klog.V(2).Infof("Session[%s] is closed", tunnelMessage.SessionID)
				return
			}

		case <-ticker.C:
			if err := s.upAgent().WsLockWriteMessage(tunnelPkg.PingMessage, []byte{}); err != nil {
				klog.V(2).Infof("Session[%s] is closed", s.SessionID)
				return
			}
		}

	}
}

func (s *Session) sessionPing(ctx context.Context) {
	defer func() {
		klog.V(2).Infof("exit sessionPing")
	}()
	ticker := time.NewTicker(tunnelPkg.PingPeriod)
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.downAgent().Context.Done():
			return
		case <-ticker.C:
			tunnelMessage := new(tunnelPkg.TunnelMessage)
			tunnelMessage.MessageType = tunnelPkg.PingMessage
			tunnelMessage.Payload = nil
			tunnelMessage.SessionID = s.SessionID
			s.downAgent().Send <- tunnelMessage
		}
	}
}

var upgrader = websocket.Upgrader{} // use default options

func (server *TunnelServer) Run() {
	go server.hub.Run()
	port := "8080"
	r := mux.NewRouter()
	r.HandleFunc("/register", server.register)
	r.HandleFunc("/", server.Proxy) //protocol/addr
	klog.Infoln("Server started at port:", port)
	klog.Fatal(http.ListenAndServe(":"+port, r))
}

func (server *TunnelServer) register(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		klog.Error("upgrade:", err)
		return
	}
	agentClientID := "test"
	ctx, cancel := context.WithCancel(context.Background())
	downClient := &Client{
		Socket:   c,
		Mu:       sync.Mutex{},
		ClientID: agentClientID,
		Send:     make(chan *tunnelPkg.TunnelMessage),
		Context:  ctx,
		Cancel:   cancel,
	}
	_, ok := server.hub.DownStreamClients.Load(agentClientID)
	if ok {
		klog.Infoln("The side agent is online")
		c.WriteMessage(websocket.TextMessage, []byte("The side agent is online"))
		c.Close()
		return
	}
	server.hub.DownStreamClients.Range(
		func(key, value interface{}) bool {
			fmt.Println(key, value)
			return true
		},
	)
	server.hub.DownStreamRegister <- downClient
	go downClient.downStreamReadPump(server.hub)
	go downClient.downStreamWritePump(server.hub)
	go downClient.downStreamPing(server.hub)
}

func (server *TunnelServer) Proxy(w http.ResponseWriter, r *http.Request) {
	var ok bool
	query := r.URL.Query()
	agentClientID := strings.ToLower(query.Get("clientid"))
	protocol := query.Get("protocol")
	address := query.Get("address")
	klog.Warningf("httpRequestinfo:%s,%s,%s", protocol, agentClientID, address)
	var c *websocket.Conn
	if protocol == tunnelPkg.ProtocolHttp {
		c = nil
	} else {
		var err error
		c, err = upgrader.Upgrade(w, r, nil)
		if err != nil {
			klog.Error("upgrade:", err)
			return
		}
	}
	klog.Warningf("ha1")
	//agentClientID := "test"
	//ws
	host := "127.0.0.1:9090"
	path := "/ws"
	//protocol := tunnelPkg.ProtocolWebsocket

	sessionId := server.SessionIDGenarator()
	upClient := &Client{
		Socket:   c,
		Mu:       sync.Mutex{},
		ClientID: sessionId,
		Send:     make(chan *tunnelPkg.TunnelMessage),
	}
	ctx, cancel := context.WithCancel(context.Background())
	session := &Session{
		SessionID: sessionId,
		UpAgent:   upClient,
		Status:    tunnelPkg.SessionConnecting,
		Cancel:    cancel,
		Protocol:  protocol,
		Mu:        sync.Mutex{},
	}
	downAgent, ok := server.hub.DownStreamClients.Load(agentClientID)
	if !ok {
		klog.Warningf("The side agent is not online")
		if protocol != tunnelPkg.ProtocolHttp {
			upClient.Socket.WriteMessage(websocket.TextMessage, []byte("The side client is not online!"))
			session.upAgent().Close()
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, "The side client is not online!")
		return
	}
	session.DownAgent = downAgent
	tunnelMessage := new(tunnelPkg.TunnelMessage)
	tunnelMessage.SessionID = session.SessionID
	tunnelMessage.Protocol = protocol
	tunnelMessage.MessageType = tunnelPkg.ConnectMessage
	//Notifies the hub that the session is created
	server.hub.SessionRegister <- session
	switch protocol {
	case tunnelPkg.ProtocolHttp:
		certs := r.TLS.PeerCertificates
		log.Println("HTTP CERTS", certs)
		defer func() {
			server.hub.SessionUnregister <- session
		}()
		httpReq, err := httpRequestToWebSocketRequest(r, address)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			klog.Warningf("httpRequestToWebSocketRequest: %v", err)
			fmt.Fprintln(w, "internal server error!")
			return
		}
		tunnelMessage.HttpRequest = httpReq
		//tunnelMessage.MessageType = tunnelPkg.BinaryMessage
		session.downAgent().Send <- tunnelMessage
		klog.V(1).Infof("session[%s] send http message to channel", session.SessionID)
		ticker := time.NewTicker(tunnelPkg.HttpRequestTimeout)
		for {
			select {
			case tunnelMessage, ok := <-session.upAgent().Send:
				if !ok {
					w.WriteHeader(http.StatusInternalServerError)
					klog.Warningf("read message error: %v", err)
					fmt.Fprintln(w, "internal server error!")
					return
				}
				httpResponse := new(tunnelPkg.HttpResponse)
				if err := json.Unmarshal(tunnelMessage.Payload, httpResponse); err != nil {
					klog.Warning("(%v)unmarshal error:%v", tunnelMessage, err)
					w.WriteHeader(http.StatusInternalServerError)
					fmt.Fprintln(w, "internal server error!")
					return
				}
				// fmt.Println(httpResponse)
				// fmt.Println("StatusCode:", httpResponse.StatusCode)
				// fmt.Println("Status:", httpResponse.Status)
				// fmt.Println("ContentType:", httpResponse.ContentType)
				// fmt.Println("Boyd:", string(httpResponse.Body))
				wsResultToHttpResponse(w, *httpResponse)
				return
			case <-ticker.C:
				w.WriteHeader(http.StatusRequestTimeout)
				fmt.Fprintln(w, "request remote server timeout!")
				return
			}
		}
	default:
		ws := new(tunnelPkg.Ws)
		ws.Host = host
		ws.Path = path
		tunnelMessage.Ws = ws
		msg, err := json.Marshal(tunnelMessage)
		if err != nil {
			klog.Warningf("The connection message marshal error: %v", err)
			upClient.Socket.WriteMessage(websocket.TextMessage, []byte(tunnelPkg.ServerInternalError))
			session.upAgent().Close()
			return
		}
		err = session.downAgent().WsLockWriteMessage(tunnelPkg.BinaryMessage, msg)
		if err != nil {
			klog.V(1).Infof("Failed to write data to agent[%s] ,%v.", session.downAgent().ClientID, err)
			session.upAgent().Close()
			return
		}
		//Notifies the hub that the session is created
		go session.upStreamReadPump(server.hub, ctx)
		go session.upStreamWritePump(server.hub, ctx)
		go session.sessionPing(ctx)
	}

}
