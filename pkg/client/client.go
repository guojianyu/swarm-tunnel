package client

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	tunnelPkg "swarm-tunnel/pkg"
	downstream "swarm-tunnel/pkg/client/downstream"
	"time"

	"k8s.io/klog"
)

func (agent *TunnelAgent) sessionSendToServer(session *Session) error {
	ticker := time.NewTicker(tunnelPkg.PingPeriod)
	defer func() {
		ticker.Stop()
		klog.V(2).Infof("exit sessionSendToServer")
	}()
	errChan := make(chan error)
	go func() {
		for {
			p := make([]byte, 1024)
			n, err := session.upAgent().in.Read(p)
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
			//send session ping
			klog.V(2).Infof("send session ping:%s", session.SessionID)
			tunnelMessage := new(tunnelPkg.TunnelMessage)
			tunnelMessage.SessionID = session.SessionID
			tunnelMessage.MessageType = tunnelPkg.PingMessage
			tunnelMessage.Payload = nil
			agent.hub.c.Send <- tunnelMessage
		case err := <-errChan:
			return err
		}
	}

}

func (agent *TunnelAgent) sessionNoticeServerClosed(sessionID string) {
	tunnelMessage := new(tunnelPkg.TunnelMessage)
	tunnelMessage.SessionID = sessionID
	tunnelMessage.MessageType = tunnelPkg.CloseMessage
	tunnelMessage.Payload = nil
	agent.hub.c.Send <- tunnelMessage
}

func (agent *TunnelAgent) httpProcessor(tunnelMessage *tunnelPkg.TunnelMessage) {

	klog.V(1).Infof("tunnelMessage.HttpRequest: %v", tunnelMessage.HttpRequest)
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
			log.Println("local http request error:", err)
			wsRes = &tunnelPkg.HttpResponse{StatusCode: http.StatusBadRequest,
				Body: []byte(err.Error())}
			return
		}
		klog.V(1).Infof("New tunnelMessage.HttpRequest: %v", localRequest)
		resp, err := (&http.Client{}).Do(localRequest)
		if err != nil {
			log.Println("local http request error:", err)
			wsRes = &tunnelPkg.HttpResponse{StatusCode: http.StatusRequestTimeout,
				Body: []byte(err.Error())}
		} else {
			wsRes, err = localResponseToWebSocketResponse(resp)
			if err != nil {
				log.Println("localResponseToWebSocketResponse:", err)
				wsRes = &tunnelPkg.HttpResponse{StatusCode: http.StatusBadRequest,
					Body: []byte(err.Error())}
			}
		}
	default:
		log.Println("tunnelMessage.HttpRequest default:", tunnelMessage.HttpRequest)
	}

}

func (agent *TunnelAgent) Processor(tunnelMessage *tunnelPkg.TunnelMessage, ctx context.Context) {
	//klog.V(2).Infof("client recv ,message:%v", tunnelMessage)
	switch tunnelMessage.MessageType {
	case tunnelPkg.PingMessage:
		return
	case tunnelPkg.ConnectMessage:
		session, err := agent.NewSessions(tunnelMessage)
		if err != nil {
			log.Println("Processor:", err)
		}
		errChan := make(chan error)
		ws := downstream.NewWsClient(session.downAgent().in, session.downAgent().out, tunnelMessage.Ws.Host, tunnelMessage.Ws.Path)
		if err := ws.Connector(); err != nil {
			klog.Warning("connect error:%v", err)
			session.Annotation = err.Error()
			agent.hub.SessionUnregister <- session
			return
		}
		go func() {
			//从上层流读数据
			errChan <- ws.DownStreamReadPump(session.Context)
		}()
		go func() {
			//向上层流写数据
			errChan <- ws.DownStreamWritePump(session.Context)
		}()

		go func() {
			errChan <- agent.sessionSendToServer(session)
		}()
		agent.hub.SessionRegister <- session
		select {
		case err := <-errChan:
			session.Annotation = err.Error()
			agent.hub.SessionUnregister <- session
		}
	case tunnelPkg.CloseMessage:
		log.Println("close message:", tunnelMessage)
		session, ok := agent.hub.Sessions.Load(tunnelMessage.SessionID)
		if !ok {
			log.Println("dont find session:")
			return
		}
		agent.hub.SessionUnregister <- session.(*Session)
	default:
		session, ok := agent.hub.Sessions.Load(tunnelMessage.SessionID)
		if !ok {
			log.Println("dont find session")
			agent.sessionNoticeServerClosed(tunnelMessage.SessionID)
		}
		//获取服务器的数据写入管道，等待下层读取
		if _, err := session.(*Session).upAgent().out.Write(tunnelMessage.Payload); err != nil {
			log.Println("session.upAgent():", err)
		}

	}
}

func (agent *TunnelAgent) NewSessions(tunnelMessage *tunnelPkg.TunnelMessage) (*Session, error) {
	downstreamReader, upstreamWriter := io.Pipe()
	upstreamReader, downstreamWriter := io.Pipe()
	upagent := &upAgent{
		out: upstreamWriter,
		in:  upstreamReader,
	}
	session := &Session{
		SessionID: tunnelMessage.SessionID,
		UpAgent:   upagent,
		Protocol:  tunnelMessage.Protocol,
	}
	session.Context, session.Cancel = context.WithCancel(context.Background())
	session.DownAgent = &upAgent{
		out: downstreamWriter,
		in:  downstreamReader,
	}

	return session, nil
}

func (agent *TunnelAgent) readServerMessage(ctx context.Context) error {
	agent.hub.c.Socket.SetReadDeadline(time.Now().Add(tunnelPkg.PongWait))
	agent.hub.c.Socket.SetPongHandler(func(string) error { agent.hub.c.Socket.SetReadDeadline(time.Now().Add(tunnelPkg.PongWait)); return nil })
	for {
		msgtype, message, err := agent.hub.c.Socket.ReadMessage()
		if err != nil {
			log.Println("client read err:", err)
			return err
		}
		//klog.V(2).Infof("client recv ,mstype:%s,message %s", msgtype, message)
		switch msgtype {
		case tunnelPkg.BinaryMessage, tunnelPkg.TextMessage:
			tunnelMessage := new(tunnelPkg.TunnelMessage)
			if err := json.Unmarshal(message, tunnelMessage); err != nil {
				log.Println("unmarshal error:", err)
				continue
			}
			switch tunnelMessage.Protocol {
			case tunnelPkg.ProtocolHttp:
				go agent.httpProcessor(tunnelMessage)
			default:
				go agent.Processor(tunnelMessage, ctx)
			}

		default:
			log.Println(msgtype)
			log.Println("do not support")
		}

	}

}

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
				log.Println("marshal error:", err)
				continue
			}
			log.Println("send message:", tunnelMessage)
			err = agent.hub.c.WsLockWriteMessage(tunnelPkg.TextMessage, message)
			if err != nil {
				log.Println("client1 write close:", err)
				return err
			}
		}
	}

}

func (agent *TunnelAgent) ping(ctx context.Context) error {
	ticker := time.NewTicker(tunnelPkg.PingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := agent.hub.c.WsLockWriteMessage(tunnelPkg.PingMessage, []byte{}); err != nil {
				log.Println("ping:", err)
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
	//done := make(chan struct{})
	go func() {
		for {
			ctx, cancel := context.WithCancel(context.Background())
			agent.hub = NewHub()
			c, err := NewProxyClient("127.0.0.1:8080", "/register")
			if err != nil {
				log.Println("connect srver error:", err)
				time.Sleep(2 * time.Second)
				continue
			}
			log.Println("connected")
			errChan := make(chan error)
			agent.hub.c = c
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
			klog.Errorf("The connection is abnormal. Please wait for reconnection.%s", err)

			cancel()
			c.Socket.Close()
			time.Sleep(tunnelPkg.PingPeriod)
		}

	}()
	select {

	case <-interrupt:
		log.Println("client interrupt")
		return
	}

}
