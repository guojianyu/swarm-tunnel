/*
Copyright 2025 The Guojianyu Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	tunnelPkg "github.com/guojianyu/swarm-tunnel/pkg"

	"github.com/gorilla/websocket"
	"k8s.io/klog"
)

func (server *TunnelServer) Run() {
	go server.hub.Run()
	//register clients
	server.Router.HandleFunc("/register", server.register)
	//routing forward
	server.Router.HandleFunc("/proxy", server.proxy)
	//list registered clients
	server.Router.HandleFunc("/clients", server.getClients).Methods("GET")
	//get all connected sessions of each client
	server.Router.HandleFunc("/sessions", server.getSessions).Methods("GET")
	if server.webserver.EnableTLS {
		klog.Infof("Starting WebSocket server with TLS on %s\n", server.webserver.Addr)
		caCert, err := ioutil.ReadFile(server.webserver.CaFile)
		if err != nil {
			klog.Fatalf("Failed to load CA certificate: %v", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		// configure TLS
		tlsConfig := &tls.Config{
			ClientCAs:  caCertPool,
			ClientAuth: tls.RequireAndVerifyClientCert,
		}
		svc := &http.Server{
			Addr:      server.webserver.Addr,
			Handler:   server.Router,
			TLSConfig: tlsConfig,
		}
		klog.Fatal(svc.ListenAndServeTLS(server.webserver.CertFile, server.webserver.KeyFile))
	} else {
		klog.Infof("Starting WebSocket server without TLS on %s\n", server.webserver.Addr)
		klog.Fatal(http.ListenAndServe(server.webserver.Addr, server.Router))
	}

}

func (server *TunnelServer) ManualDisconnenctClient(clientId string) error {
	if client, ok := server.hub.ByIdGetClient(clientId); !ok {
		return fmt.Errorf("The [%v]clientid is not connected", clientId)
	} else {
		client.Cancel()
	}
	return nil
}

func (server *TunnelServer) getClients(w http.ResponseWriter, r *http.Request) {
	clients := []string{}
	server.hub.DownStreamClients.Range(
		func(key, value interface{}) bool {
			clients = append(clients, key.(string))
			return true
		},
	)
	jsonData, err := json.Marshal(clients)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)
}

func (server *TunnelServer) getSessions(w http.ResponseWriter, r *http.Request) {
	//key:client  value:session lists
	session := make(map[string][]string)
	server.hub.Sessions.Range(
		func(key, value interface{}) bool {
			client := value.(*Session).DownAgent.(*Client).ClientID
			if _, ok := session[client]; !ok {
				session[client] = []string{}
			}
			session[client] = append(session[client], key.(string))
			return true
		},
	)
	jsonData, err := json.Marshal(session)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)
}

func (server *TunnelServer) register(w http.ResponseWriter, r *http.Request) {
	queryParams := r.URL.Query()
	agent := queryParams.Get("clientid")
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		klog.Error("upgrade:", err)
		return
	}
	if server.hub.registerClientHandle != nil {
		if err := server.hub.registerClientHandle(agent, r); err != nil {
			klog.V(4).Infof("Active disconnection,dynamic registerClientHandle error:%v", err)
			c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("Server shutting down,%v", err)))
			c.Close()
			return
		}
	}
	agentClientID := agent
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
		//klog.Infoln("Server shutting down,The side agent is online!")
		c.WriteMessage(websocket.TextMessage, []byte("Server shutting down,The side agent is online"))
		downClient.close()
		return
	}
	server.hub.DownStreamRegister <- downClient
	defer func() {
		klog.V(4).Infof("client[%s] defer", downClient.ClientID)

		server.hub.DownStreamUnregister <- downClient
	}()
	go downClient.readDownStreamPump(server.hub)
	go downClient.writeDownStreamPump(server.hub)
	downClient.pingDownStream(server.hub)
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func (server *TunnelServer) proxy(w http.ResponseWriter, r *http.Request) {
	var ok bool
	query := r.URL.Query()
	agentClientID := strings.ToLower(query.Get("clientid"))
	address := query.Get("address")
	tmp, err := url.Parse(address)
	if err != nil {
		info := fmt.Sprintf("Error parsing address %s: %v\n", address, err)
		fmt.Fprintln(w, info)
		return
	}
	protocol := tmp.Scheme
	host := tmp.Host
	path := tmp.Path
	// Split host into address and port
	ip, port, _ := strings.Cut(host, ":")
	klog.Warningf("Request info: agent[%s] protocol[%s] host[ip %s port %s]  path[%s]", agentClientID, protocol, ip, port, path)
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
	sessionId := server.sessionIDGenarator()
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
		Context:   ctx,
		Protocol:  protocol,
		Mu:        sync.Mutex{},
	}
	defer func() {
		//Registered sessions are handled by the hub
		klog.V(4).Infof("session[%s] defer", session.SessionID)
		if _, ok := server.hub.ByIdGetSession(session.SessionID); ok {
			server.hub.SessionUnregister <- session
		} else {
			//If the session is not registered ,the socket connection and channel are manually closed
			session.close()
		}
	}()
	downAgent, ok := server.hub.DownStreamClients.Load(agentClientID)
	if !ok {
		klog.Warningf("The side agent is not online")
		if protocol != tunnelPkg.ProtocolHttp {
			session.upAgent().Socket.WriteMessage(websocket.TextMessage, []byte("The side client is not online!"))
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintln(w, "The side client is not online!")
		return
	}
	session.DownAgent = downAgent
	tunnelMessage := new(tunnelPkg.TunnelMessage)
	tunnelMessage.SessionID = session.SessionID
	tunnelMessage.Protocol = protocol
	tunnelMessage.MessageType = tunnelPkg.ConnectMessage

	switch protocol {
	case tunnelPkg.ProtocolHttp:
		//Notifies the hub that the session is created
		server.hub.SessionRegister <- session
		//certs := r.TLS.PeerCertificates
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
	case tunnelPkg.ProtocolSSH:
		ssh := new(tunnelPkg.SSH)
		var err error
		ssh.Host = ip
		ssh.Port = port
		ssh.User, ssh.Password, err = session.userSshInteraction(ssh.Host)
		if err != nil {
			return
		}
		//ssh.User = "root"
		//ssh.Password = "AfDX{8Ag"
		klog.V(4).Infof("ssh login info: %v", ssh)
		tunnelMessage.SSH = ssh
	case tunnelPkg.ProtocolWebsocket:
		ws := new(tunnelPkg.Ws)
		ws.Host = host
		ws.Path = path
		tunnelMessage.Ws = ws
	default:
		ws := new(tunnelPkg.Ws)
		ws.Host = host
		ws.Path = path
		tunnelMessage.Ws = ws

	}
	msg, err := json.Marshal(tunnelMessage)
	if err != nil {
		klog.Warningf("The connection message marshal error: %v", err)
		upClient.Socket.WriteMessage(websocket.TextMessage, []byte(tunnelPkg.ServerInternalError))
		return
	}
	err = session.downAgent().WsLockWriteMessage(tunnelPkg.BinaryMessage, msg)
	if err != nil {
		klog.V(1).Infof("Failed to write data to agent[%s] ,%v.", session.downAgent().ClientID, err)
		return
	}
	//Notifies the hub that the session is created
	server.hub.SessionRegister <- session
	//Start data processing
	go session.readUpStreamPump(server.hub)
	session.writeUpStreamPump(server.hub)

}

func (c *Client) readDownStreamPump(hub *Hub) {
	c.Socket.SetReadDeadline(time.Now().Add(tunnelPkg.PongWait))
	c.Socket.SetPongHandler(func(string) error { c.Socket.SetReadDeadline(time.Now().Add(tunnelPkg.PongWait)); return nil })
	defer func() {
		klog.V(4).Infof("[%s] exit readDownStreamPump", c.ClientID)
		c.Cancel()
	}()
	for {
		select {
		case <-c.Context.Done():
			return
		default:
			msgtype, message, err := c.Socket.ReadMessage()
			if err != nil {
				// if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				// 	klog.V(1).Infof("Failed to read data from downstream,%v.", err)
				// 	break
				// }
				klog.V(4).Infof("Failed to read data from downstream[%s] ,%v.", c.ClientID, err)
				return
			}

			switch msgtype {
			case tunnelPkg.PingMessage:
				klog.V(4).Infof("ping.")
			default:
				tunnelMessage := new(tunnelPkg.TunnelMessage)
				if err := json.Unmarshal(message, tunnelMessage); err != nil {
					klog.Warning("(%v)unmarshal error:%v", message, err)
					continue
				}
				sessionID := tunnelMessage.SessionID
				if session, ok := hub.ByIdGetSession(sessionID); ok {
					//klog.V(4).Infof("Session[%s] from agent[%s] received message: %v,payload: %v", sessionID, c.ClientID, tunnelMessage, string(tunnelMessage.Payload))
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
}
func (c *Client) writeDownStreamPump(hub *Hub) {
	defer func() {
		klog.V(4).Infof("[%s] exit writeDownStreamPump", c.ClientID)
		c.Cancel()
	}()
	for {
		select {
		case <-c.Context.Done():
			return
		case tunnelMessage, ok := <-c.Send:
			if !ok {
				klog.V(4).Infof("downStreamWritePump,%v.", ok)
				return
			}
			//klog.V(4).Infof("Session[%s] to agent[%s] send message: %v", tunnelMessage.SessionID, c.ClientID, tunnelMessage)
			msg, err := json.Marshal(tunnelMessage)
			if err != nil {
				klog.Warning("(%v)Marshal error: %v", tunnelMessage, err)
				continue
			}
			if err := c.WsLockWriteMessage(tunnelPkg.TextMessage, msg); err != nil {
				klog.V(4).Infof("Failed to write data to agent[%s] ,%v.", c.ClientID, err)
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

func (c *Client) pingDownStream(hub *Hub) {
	defer func() {
		klog.V(4).Infof("[%s] exit pingDownStream", c.ClientID)
	}()
	ticker := time.NewTicker(tunnelPkg.PingPeriod)
	for {
		select {
		case <-ticker.C:
			if err := c.WsLockWriteMessage(tunnelPkg.PingMessage, []byte{}); err != nil {
				klog.V(4).Infof("Failed to write data to agent[%s] ,%v.", c.ClientID, err)
				return
			}
		case <-c.Context.Done():
			return
		}
	}
}

func (s *Session) readUpStreamPump(hub *Hub) {
	s.upAgent().Socket.SetReadDeadline(time.Now().Add(tunnelPkg.PongWait))
	s.upAgent().Socket.SetPongHandler(func(string) error { s.upAgent().Socket.SetReadDeadline(time.Now().Add(tunnelPkg.PongWait)); return nil })
	defer func() {
		klog.V(4).Infof("exit upStreamReadPump")
		s.Cancel()
	}()
	for {
		select {
		case <-s.Context.Done():
			klog.V(4).Infof("readUpStreamPump ctx done")
			return
		case <-s.downAgent().Context.Done():
			klog.V(4).Infof("readUpStreamPump downagent ctx done")
			return
		default:
			msgtype, message, err := s.upAgent().Socket.ReadMessage()
			if err != nil {
				// if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				// 	klog.Error("error: ", err)
				// }
				klog.Error("error: ", err)
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

	}

}

func (s *Session) writeUpStreamPump(hub *Hub) {
	defer func() {
		klog.V(4).Infof("exit upStreamWritePump")
		s.Cancel()
	}()
	ticker := time.NewTicker(tunnelPkg.PingPeriod)
	for {
		select {
		case <-s.Context.Done():
			return
		case <-s.downAgent().Context.Done():
			klog.V(4).Infof("writeUpStreamPump downagent ctx done")
			return
		case tunnelMessage, ok := <-s.upAgent().Send:
			if !ok {
				// The hub closed the channel.
				//c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			klog.V(4).Infof("Session[%s] from agent[%s] received message: %v,payload: %s", tunnelMessage.SessionID, s.downAgent().ClientID, tunnelMessage, tunnelMessage.Payload)
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
				s.Annotation = string(tunnelMessage.Payload)
				return
			case tunnelPkg.PingMessage:
				continue
			}
			err := s.upAgent().WsLockWriteMessage(tunnelPkg.TextMessage, tunnelMessage.Payload)
			if err != nil {
				klog.V(4).Infof("Session[%s] is closed", tunnelMessage.SessionID)
				return
			}

		case <-ticker.C:
			if err := s.upAgent().WsLockWriteMessage(tunnelPkg.PingMessage, []byte{}); err != nil {
				klog.V(4).Infof("Session[%s] is closed", s.SessionID)
				return
			}
		}

	}
}

func (s *Session) userSshInteraction(host string) (user, password string, err error) {
	auth := map[string]string{}
	_, message, err := s.upAgent().Socket.ReadMessage()
	if err != nil {
		return
	}
	//Have you configured passwordless login, skipping username and password authentication?
	//Y(yes) ssh configures passwordless login
	//N or other(No) log in to ssh using the username and password
	if strings.ToLower(strings.TrimSpace(string(message))) == "y" {
		return
	}
	_, message, err = s.upAgent().Socket.ReadMessage()
	if err != nil {
		return
	}
	if err = json.Unmarshal(message, &auth); err != nil {
		klog.Errorf("JSON Parse Error:%v", err)
		return
	}
	user = auth["username"]
	password = auth["password"]
	return
}
