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
package client

import (
	"context"
	"encoding/json"

	tunnelPkg "github.com/guojianyu/swarm-tunnel/pkg"

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
				klog.V(4).Infof("marshal error:%v", err)
				continue
			}
			klog.V(4).Infof("send connection message: %s", msg)
			hub.c.Send <- tunnelMessage
			// if err := hub.c.WsLockWriteMessage(tunnelPkg.TextMessage, msg); err != nil {
			// 	klog.V(1).Infof("send close session error: %v", err)
			// }
			klog.V(4).Infof("The session[%s] is created", session.SessionID)
		case session := <-hub.SessionUnregister:
			klog.V(4).Infof("The session[%s] is removed", session.SessionID)
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
