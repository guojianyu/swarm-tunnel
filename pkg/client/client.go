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
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	tunnelPkg "github.com/guojianyu/swarm-tunnel/pkg"
	downStream "github.com/guojianyu/swarm-tunnel/pkg/client/downstream"

	"k8s.io/klog"
)

/*
Data is read from the upper stream and then written to the client channel, waiting for unified processing
*/
func (agent *TunnelAgent) sessionSendToServer(session *Session) error {
	ticker := time.NewTicker(tunnelPkg.PingPeriod)
	defer func() {
		ticker.Stop()
		klog.V(2).Infof("exit sessionSendToServer:%v", session.SessionID)
	}()
	errChan := make(chan error)
	go func() {
		for {
			p := make([]byte, 1024)
			n, err := session.upAgent().out.Read(p)
			if err != nil {
				errChan <- err
				return
			}
			tunnelMessage := new(tunnelPkg.TunnelMessage)
			tunnelMessage.SessionID = session.SessionID
			tunnelMessage.MessageType = tunnelPkg.BinaryMessage
			tunnelMessage.Payload = p[:n]
			agent.hub.c.Send <- tunnelMessage

		}

	}()
	for {
		select {
		case <-session.Context.Done():
			return nil
		case <-ticker.C:
			//send session ping,这部分改造，不发送session ping，逻辑改为间隔时间未检查到数据交互就断联
			// klog.V(2).Infof("send session ping:%s", session.SessionID)
			// tunnelMessage := new(tunnelPkg.TunnelMessage)
			// tunnelMessage.SessionID = session.SessionID
			// tunnelMessage.MessageType = tunnelPkg.PingMessage
			// tunnelMessage.Payload = nil
			// agent.hub.c.Send <- tunnelMessage
		case err := <-errChan:
			return err
		}
	}

}

/*
Send a sesssion closure message to the server
*/
func (agent *TunnelAgent) sessionNoticeServerClosed(sessionID string) {
	tunnelMessage := new(tunnelPkg.TunnelMessage)
	tunnelMessage.SessionID = sessionID
	tunnelMessage.MessageType = tunnelPkg.CloseMessage
	tunnelMessage.Payload = nil
	agent.hub.c.Send <- tunnelMessage
}

/*
Handling http protocol
*/
func (agent *TunnelAgent) httpProcessor(tunnelMessage *tunnelPkg.TunnelMessage) {
	klog.V(2).Infof("tunnelMessage.HttpRequest: %v", tunnelMessage.HttpRequest)
	switch tunnelMessage.MessageType {
	case tunnelPkg.ConnectMessage:
		wsRes := &tunnelPkg.HttpResponse{}
		defer func() {
			message, _ := json.Marshal(wsRes)
			tunnelMessage.Payload = message
			agent.hub.c.Send <- tunnelMessage
		}()

		localRequest, err := wsToLocalRequest(tunnelMessage.HttpRequest)
		if err != nil {
			klog.Errorf("local http request error: %v", err)
			wsRes = &tunnelPkg.HttpResponse{StatusCode: http.StatusBadRequest,
				Body: []byte(err.Error())}
			return
		}
		klog.V(2).Infof("New tunnelMessage.HttpRequest: %v", localRequest)
		resp, err := (&http.Client{}).Do(localRequest)
		if err != nil {
			klog.Errorf("local http request error: %v", err)
			wsRes = &tunnelPkg.HttpResponse{StatusCode: http.StatusRequestTimeout,
				Body: []byte(err.Error())}
		} else {
			wsRes, err = localResponseToWebSocketResponse(resp)
			if err != nil {
				klog.Errorf("localResponseToWebSocketResponse: %v", err)
				wsRes = &tunnelPkg.HttpResponse{StatusCode: http.StatusBadRequest,
					Body: []byte(err.Error())}
			}
		}
	default:
		klog.Infoln("tunnelMessage.HttpRequest default:", tunnelMessage.HttpRequest)
	}

}

/*
All protocols except http are uniformly processed by this function
*/
func (agent *TunnelAgent) Processor(tunnelMessage *tunnelPkg.TunnelMessage, ctx context.Context) {
	//klog.V(2).Infof("client recv ,message:%v", tunnelMessage)
	switch tunnelMessage.MessageType {
	case tunnelPkg.PingMessage:
		return
	case tunnelPkg.ConnectMessage:
		session := agent.NewSessions(tunnelMessage)
		errChan := make(chan error)
		/*
			Extend the downstream protocol based on the interface of the downstream directory
		*/
		downstream, err := downStream.NewDownStream(tunnelMessage)
		if err != nil {
			klog.Warning("connect error:%v", err)
			session.Annotation = err.Error()
			agent.hub.SessionUnregister <- session
			return
		}
		//Read data from the upper stream and written to the down stream
		go func() {
			errChan <- func(session *Session) error {
				p := make([]byte, 1024)
				for {
					select {
					case <-ctx.Done():
						klog.Infof("Session[%s] downstream read exit", session.SessionID)
						return nil
					case <-session.Context.Done():
						return nil
					default:
						n, err := session.downAgent().out.Read(p)
						if err != nil {
							//Close half of the session's pipes, this will return an error and exit
							klog.Errorf("Session[%s] downstream read error:%v", session.SessionID, err)
							return err
						}
						klog.V(2).Infof("Session[%s] downstream read: %v", session.SessionID, string(p[:n]))
						err = downstream.Receive(p[:n])
						if err != nil {
							klog.Errorf("Session[%s] downstream read error:%v", session.SessionID, err)
							return err
						}
					}
				}
			}(session)
		}()

		//Data is read from the down stream and written to the upper stream
		go func() {
			errChan <- func(session *Session) error {
				for {
					select {
					case <-ctx.Done():
						klog.Infof("Session[%s] downstream write exit", session.SessionID)
						return nil
					case <-session.Context.Done():
						return nil
					default:
						message, err := downstream.Send()
						if err != nil {
							klog.Errorf("Session[%s] downstream write error:%v", session.SessionID, err)
							return err
						}
						_, err = session.downAgent().in.Write(message)
						klog.V(2).Infof("Session[%s] downstream write: %v", session.SessionID, string(message))
						if err != nil {
							klog.Errorf("Session[%s] downstream write error:%v", session.SessionID, err)
							return err
						}
					}
				}
			}(session)
		}()

		go func() {
			errChan <- agent.sessionSendToServer(session)
		}()
		agent.hub.SessionRegister <- session
		select {
		case err := <-errChan:
			//close the down stream connection
			downstream.Close()
			if err != nil {
				session.setAnnotaion(err.Error())
			}
			session.setStatus(tunnelPkg.SessionProcessFailed)
			agent.hub.SessionUnregister <- session
			break
		}
	case tunnelPkg.CloseMessage:
		klog.Infof("close message: %v", tunnelMessage)
		s, ok := agent.hub.Sessions.Load(tunnelMessage.SessionID)
		if !ok {
			klog.Errorf("Do not find session:%s", tunnelMessage.SessionID)
			return
		}
		session := s.(*Session)
		session.setStatus(tunnelPkg.SessionUpstreamClosed)
		session.close()
		//session.Cancel()
	default:
		session, ok := agent.hub.Sessions.Load(tunnelMessage.SessionID)
		if !ok {
			klog.Errorf("Do not find session:%s", tunnelMessage.SessionID)
			agent.sessionNoticeServerClosed(tunnelMessage.SessionID)
			return
		}
		//Gets the server's data write upper stream and waits for the down stream to read it
		if _, err := session.(*Session).upAgent().in.Write(tunnelMessage.Payload); err != nil {
			klog.Errorf("The session write upper stream error: %v", err)
		}

	}
}

/*
New session
*/
func (agent *TunnelAgent) NewSessions(tunnelMessage *tunnelPkg.TunnelMessage) *Session {
	downstreamReader, upstreamWriter := io.Pipe()
	upstreamReader, downstreamWriter := io.Pipe()
	session := &Session{
		SessionID: tunnelMessage.SessionID,
		UpAgent: &pump{
			in:  upstreamWriter,
			out: upstreamReader,
		},
		DownAgent: &pump{
			in:  downstreamWriter,
			out: downstreamReader,
		},
		Protocol: tunnelMessage.Protocol,
	}
	session.Context, session.Cancel = context.WithCancel(context.Background())
	return session
}

/*
The data is read from the server and processed by classification
http is special, and other protocols are handled by Processor function
*/
func (agent *TunnelAgent) readServerMessage(ctx context.Context) error {
	agent.hub.c.Socket.SetReadDeadline(time.Now().Add(tunnelPkg.PongWait))
	agent.hub.c.Socket.SetPongHandler(func(string) error { agent.hub.c.Socket.SetReadDeadline(time.Now().Add(tunnelPkg.PongWait)); return nil })
	for {
		msgtype, message, err := agent.hub.c.Socket.ReadMessage()
		if err != nil {
			// if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
			// 	klog.Errorf("client read err: %v", err)
			// 	return err
			// }
			klog.Errorf("client read err: %v", err)
			return err
		}
		klog.V(2).Infof("client recv ,type:%v,message %s", msgtype, message)
		switch msgtype {
		case tunnelPkg.BinaryMessage, tunnelPkg.TextMessage:
			tunnelMessage := new(tunnelPkg.TunnelMessage)
			if err := json.Unmarshal(message, tunnelMessage); err != nil {
				if strings.HasPrefix(string(message), "Server shutting down") {
					klog.Infof("%s", string(message))
				} else {
					klog.Errorf("unmarshal error: %v", err)
				}
				continue
			}
			switch tunnelMessage.Protocol {
			case tunnelPkg.ProtocolHttp:
				go agent.httpProcessor(tunnelMessage)
			default:
				go agent.Processor(tunnelMessage, ctx)
			}

		default:
			klog.Error("Do not support")
		}

	}

}

/*
All session data is written to the client channel, and data is read from the client channel and sent to the server.
Complex  logic can be added here in the future.
*/
func (agent *TunnelAgent) writeServerMessage(ctx context.Context) error {
	for {
		select {
		// if the goroutine is done , all are out
		case <-ctx.Done():
			return nil
		case tunnelMessage, ok := <-agent.hub.c.Send:
			if !ok {
				// The hub closed the channel.
				return nil
			}
			message, err := json.Marshal(tunnelMessage)
			if err != nil {
				klog.Errorf("marshal error: %v, message content: %v", err.Error(), message)
				continue
			}
			err = agent.hub.c.WsLockWriteMessage(tunnelPkg.TextMessage, message)
			if err != nil {
				klog.Errorf("The client failed to write message to the server. %v:", err.Error())
				return err
			}
			klog.V(2).Infof("The client send a message: %v, payload:%v", tunnelMessage, string(tunnelMessage.Payload))
		}
	}

}

/*
ping server
*/
func (agent *TunnelAgent) ping(ctx context.Context) error {
	ticker := time.NewTicker(tunnelPkg.PingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := agent.hub.c.WsLockWriteMessage(tunnelPkg.PingMessage, []byte{}); err != nil {
				klog.Errorf("The client ping error:%v", err.Error())
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (agent *TunnelAgent) Run() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	go func() {
		for {
			ctx, cancel := context.WithCancel(context.Background())
			err := agent.newWebSocketClient()
			if err != nil {
				klog.Errorf("connect srver error:%v", err.Error())
				time.Sleep(2 * time.Second)
				continue
			}
			klog.Infof("Connected WebSocket server: %s\n", agent.webserver.Addr)
			errChan := make(chan error)
			go agent.hub.Run(ctx)
			go func() {
				errChan <- agent.ping(ctx)
			}()
			go func() {
				errChan <- agent.writeServerMessage(ctx)
			}()
			go func() {
				errChan <- agent.readServerMessage(ctx)
			}()
			err = <-errChan
			klog.Errorf("The connection is abnormal,%s. Please wait for reconnection.", err.Error())
			cancel()
			agent.hub.c.Socket.Close()
			time.Sleep(10 * time.Second)
		}

	}()
	select {
	case <-interrupt:
		klog.Infoln("client interrupt")
		return
	}

}
