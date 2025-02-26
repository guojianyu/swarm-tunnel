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
	"encoding/json"
	"fmt"

	tunnelPkg "github.com/guojianyu/swarm-tunnel/pkg"

	"k8s.io/klog"
)

func (hub *Hub) Run() {
	klog.Infof("The hub is stared")
	defer func() {
		klog.Infof("The hub is exited")
	}()
	for {
		select {
		case client := <-hub.DownStreamRegister:
			hub.DownStreamClients.Store(client.ClientID, client)
			if hub.registerClientCallback != nil {
				hub.registerClientCallback(client.ClientID)
			}
			klog.V(1).Infof("Agent[%s] is successfully registered.", client.ClientID)
		case client := <-hub.DownStreamUnregister:
			klog.V(1).Infof("Agent[%s] is disconnected start.", client.ClientID)
			_, ok := hub.DownStreamClients.Load(client.ClientID)
			if !ok {
				klog.Errorf("Agent[%s] load error.", client.ClientID)
				continue
			}
			klog.V(1).Infof("Agent[%s] is disconnected mid.", client.ClientID)
			client.close()
			klog.V(1).Infof("Agent[%s] is disconnected delete.", client.ClientID)
			hub.DownStreamClients.Delete(client.ClientID)
			if hub.unregisterClientCallback != nil {
				hub.unregisterClientCallback(client.ClientID)
			}
			klog.V(1).Infof("Agent[%s] is disconnected.", client.ClientID)
		case session := <-hub.SessionRegister:
			hub.Sessions.Store(session.SessionID, session)
			info := fmt.Sprintf("The session[%s] with the agent[%s] is %s ", session.SessionID, session.downAgent().ClientID, session.GetStatus())
			klog.V(1).Infof(info)
			if hub.registerSessionCallback != nil && session.GetStatus() == tunnelPkg.SessionConnected {
				hub.registerSessionCallback(session.SessionID)
			}
			if session.Protocol != tunnelPkg.ProtocolHttp {
				session.upAgent().WsLockWriteMessage(tunnelPkg.TextMessage, []byte(info))
			}
		case session := <-hub.SessionUnregister:
			if _, ok := hub.Sessions.Load(session.SessionID); !ok {
				continue
			}
			hub.Sessions.Delete(session.SessionID)
			klog.V(1).Infof("The protocol[%s] session[%s] with the agent[%s] is removed %v", session.Protocol, session.SessionID, session.downAgent().ClientID, session.Annotation)
			if session.Protocol == tunnelPkg.ProtocolHttp {
				continue
			}
			if hub.unregisterSessionCallback != nil {
				hub.unregisterSessionCallback(session.SessionID)
			}
			if session.Status != tunnelPkg.SessionUpstreamClosed {
				session.upAgent().WsLockWriteMessage(tunnelPkg.TextMessage, []byte(session.Annotation))
			} else {
				//upstream is closed
				tunnelMessage := tunnelPkg.GenerateSessionClosedMessage(session.SessionID, string(session.Status))
				msg, err := json.Marshal(tunnelMessage)
				if err != nil {
					klog.Warning("(%v)Marshal error: %v", tunnelMessage, err)
				} else {
					session.downAgent().WsLockWriteMessage(tunnelPkg.TextMessage, msg)
				}
				// if !session.downAgent().IsClosed {
				// 	session.downAgent().Send <- tunnelPkg.GenerateSessionClosedMessage(session.SessionID, string(session.Status))
				// }
			}
			session.close()
		}
	}
}
