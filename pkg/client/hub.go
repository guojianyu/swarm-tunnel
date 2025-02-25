package client

import (
	"context"
	"encoding/json"
	tunnelPkg "swarm-tunnel/pkg"

	"k8s.io/klog"
)

func (hub *Hub) Run(ctx context.Context) {
	klog.Infof("The hub is stared")
	for {
		select {
		case <-ctx.Done():
			klog.Infof("The hub exit")
			return
		case session := <-hub.SessionRegister:
			hub.Sessions.Store(session.SessionID, session)
			tunnelMessage := new(tunnelPkg.TunnelMessage)
			tunnelMessage.SessionID = session.SessionID
			tunnelMessage.MessageType = tunnelPkg.ConnectMessage
			msg, err := json.Marshal(tunnelMessage)
			if err != nil {
				klog.V(2).Infof("marshal error:%v", err)
				continue
			}
			klog.V(2).Infof("send connection message: %s", msg)
			hub.c.Send <- tunnelMessage
			// if err := hub.c.WsLockWriteMessage(tunnelPkg.TextMessage, msg); err != nil {
			// 	klog.V(1).Infof("send close session error: %v", err)
			// }
			klog.V(1).Infof("The session[%s] is created", session.SessionID)
		case session := <-hub.SessionUnregister:
			klog.Infof("The session[%s] is removed", session.SessionID)
			if _, ok := hub.Sessions.Load(session.SessionID); ok {
				hub.Sessions.Delete(session.SessionID)
				session.close()
			}
			//Notifying the server of session creation failure
			if session.Status != tunnelPkg.SessionUpstreamClosed {
				tunnelMessage := new(tunnelPkg.TunnelMessage)
				tunnelMessage.SessionID = session.SessionID
				tunnelMessage.MessageType = tunnelPkg.CloseMessage
				tunnelMessage.Payload = []byte(session.Annotation)
				hub.c.Send <- tunnelMessage
			}

		}
	}
}
