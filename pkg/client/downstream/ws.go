package downstream

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	tunnelPkg "swarm-tunnel/pkg"
	"time"

	"github.com/gorilla/websocket"
)

func NewWsClient(in *io.PipeReader, out *io.PipeWriter, host, path string) *WsClient {
	return &WsClient{in: in, out: out, host: host, path: path}
}

func (ws *WsClient) Connector() error {
	log.Printf("ws Connector")
	u := url.URL{Scheme: "ws", Host: ws.host, Path: ws.path}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	ws.Socket = c
	return err
}

func (ws *WsClient) Processor(ctx context.Context) error {
	defer func() {
		ws.Socket.Close()
	}()
	ch := make(chan error, 1)
	//ctx, cancle := context.WithCancel(context.Background())
	go func() {
		ch <- ws.DownStreamReadPump(ctx)
	}()

	go func() {
		ch <- ws.DownStreamWritePump(ctx)
	}()
	for {
		select {
		case <-ctx.Done():
			log.Printf("exit")
			return nil
		case err := <-ch:
			return err
		}
	}
	return nil

}

func (ws *WsClient) DownStreamReadPump(ctx context.Context) error {
	p := make([]byte, 1024)
	for {
		select {
		case <-ctx.Done():
			log.Printf("exit")
			return nil
		default:
			n, err := ws.in.Read(p)
			if err != nil {
				//关闭session的其中一半管道，这个会返回错误然后退出
				return err
			}
			log.Printf("ws recv%v", string(p[:n]))
			ws.Socket.SetWriteDeadline(time.Now().Add(tunnelPkg.WriteWait))
			err = ws.Socket.WriteMessage(tunnelPkg.BinaryMessage, p[:n])
			if err != nil {
				log.Println("client write close:", err)
				return fmt.Errorf("The downstream is disconnected:", err)
			}
		}
	}
}

func (ws *WsClient) DownStreamWritePump(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Printf("exit")
			return nil
		default:
			_, message, err := ws.Socket.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("error: %v", err)
					return fmt.Errorf("The downstream is disconnected:", err)
				}
				return err
			}
			_, err = ws.out.Write(message)
			log.Printf("ws send %v", string(message))
			if err != nil {
				log.Println("error:", err)
				return err
			}
		}
	}
}
