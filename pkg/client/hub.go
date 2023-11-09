package client

import (
	"context"
	"encoding/json"
	"log"
	tunnelPkg "swarm-tunnel/pkg"
	"sync"

	"k8s.io/klog"
)

func NewHub() *Hub {
	return &Hub{
		SessionRegister:   make(chan *Session),
		SessionUnregister: make(chan *Session),
		Sessions:          sync.Map{},
	}
}

func NewTunnelAgent() *TunnelAgent {
	return &TunnelAgent{}
}

func (hub *Hub) Run(ctx context.Context) {
	klog.Infof("Hub stared")
	for {
		select {
		case <-ctx.Done():
			log.Printf("hub exit")
			return
		case session := <-hub.SessionRegister:
			hub.Sessions.Store(session.SessionID, session)
			tunnelMessage := new(tunnelPkg.TunnelMessage)
			tunnelMessage.SessionID = session.SessionID
			tunnelMessage.MessageType = tunnelPkg.ConnectMessage

			msg, err := json.Marshal(tunnelMessage)
			if err != nil {
				klog.V(1).Infof("marshal error:%v", err)
				continue
			}
			klog.V(1).Infof("send connection message: %s", msg)
			hub.c.Send <- tunnelMessage
			// if err := hub.c.WsLockWriteMessage(tunnelPkg.TextMessage, msg); err != nil {
			// 	klog.V(1).Infof("send close session error: %v", err)
			// }
			klog.V(1).Infof("The session[%s] is created", session.SessionID)
		case session := <-hub.SessionUnregister:
			if _, ok := hub.Sessions.Load(session.SessionID); !ok {
				continue
			}
			klog.Infof("The session[%s] is removed", session.SessionID)
			hub.Sessions.Delete(session.SessionID)
			session.close()
			tunnelMessage := new(tunnelPkg.TunnelMessage)
			tunnelMessage.SessionID = session.SessionID
			tunnelMessage.MessageType = tunnelPkg.CloseMessage
			tunnelMessage.Payload = []byte(session.Annotation)
		}
	}
}
