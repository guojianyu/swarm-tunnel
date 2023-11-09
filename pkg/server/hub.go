package server

import (
	"fmt"
	tunnelPkg "swarm-tunnel/pkg"
	"sync"

	"k8s.io/klog"
)

func NewHub() *Hub {
	return &Hub{
		UpStreamRegister:     make(chan *Client),
		UpStreamUnregister:   make(chan *Client),
		DownStreamRegister:   make(chan *Client),
		DownStreamUnregister: make(chan *Client),
		SessionRegister:      make(chan *Session),
		SessionUnregister:    make(chan *Session),
		UpStreamClients:      sync.Map{},
		DownStreamClients:    sync.Map{},
		Sessions:             sync.Map{},
	}
}

func NewTunnelServer() *TunnelServer {
	return &TunnelServer{hub: NewHub()}
}

func (hub *Hub) Run() {
	klog.Infof("Hub stared")
	for {
		select {
		case client := <-hub.DownStreamRegister:
			hub.DownStreamClients.Store(client.ClientID, client)
			klog.V(1).Infof("Agent[%s] is successfully registered.", client.ClientID)
		case client := <-hub.DownStreamUnregister:
			c, ok := hub.DownStreamClients.Load(client.ClientID)
			if ok {
				client, ok = converseClient(c)
				if !ok {
					continue
				}
			}

			hub.DownStreamClients.Delete(client.ClientID)
			client.Cancel()
			//	client.WsLockWriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			client.Socket.Close()
			klog.V(1).Infof("Agent[%s] is disconnected.", client.ClientID)
		case session := <-hub.SessionRegister:
			hub.Sessions.Store(session.SessionID, session)
			var action string
			if session.GetStatus() == tunnelPkg.SessionConnecting {
				action = "created"
			} else if session.GetStatus() == tunnelPkg.SessionConnected {
				action = "connected"
			}
			info := fmt.Sprintf("The session[%s] with the agent[%s] is %s", session.SessionID, session.downAgent().ClientID, action)
			klog.V(1).Infof(info)
			if session.Protocol != tunnelPkg.ProtocolHttp {
				session.upAgent().WsLockWriteMessage(tunnelPkg.TextMessage, []byte(info))
			}

		case session := <-hub.SessionUnregister:
			klog.V(1).Infof("The protocol[%s] session[%s] with the agent[%s] is removed", session.Protocol, session.SessionID, session.downAgent().ClientID)
			if session.Protocol == tunnelPkg.ProtocolHttp {
				hub.Sessions.Delete(session.SessionID)
				continue
			}
			hub.Sessions.Delete(session.SessionID)
			session.Cancel()
			session.upAgent().Close()
		}
	}
}
