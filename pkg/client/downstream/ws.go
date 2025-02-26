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
package downstream

import (
	"fmt"
	"log"
	"net/url"
	"time"

	tunnelPkg "github.com/guojianyu/swarm-tunnel/pkg"

	"github.com/gorilla/websocket"
)

type WsClient struct {
	Socket *websocket.Conn
}

const protocol = "websocket"

func NewWsClient(tunnelMessage *tunnelPkg.TunnelMessage) (*WsClient, error) {
	ws := &WsClient{}
	//log.Printf("ws Connector")
	u := url.URL{Scheme: "ws", Host: tunnelMessage.Ws.Host, Path: tunnelMessage.Ws.Path}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	ws.Socket = c
	return ws, err
}

func (ws *WsClient) Protocol() string {
	return protocol
}

func (ws *WsClient) Receive(data []byte) error {
	ws.Socket.SetWriteDeadline(time.Now().Add(tunnelPkg.WriteWait))
	err := ws.Socket.WriteMessage(tunnelPkg.BinaryMessage, data)
	if err != nil {
		log.Println("client write close:", err)
		return fmt.Errorf("The downstream is disconnected: %v", err)
	}
	return nil
}

func (ws *WsClient) Send() (data []byte, err error) {
	_, message, err := ws.Socket.ReadMessage()
	if err != nil {
		if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
			log.Printf("error: %v", err)
		}
	}
	return message, err
}

func (ws *WsClient) Close() error {
	log.Println("ws is closed")
	return ws.Socket.Close()
}
