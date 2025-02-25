package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"

	"github.com/emicklei/go-restful/v3"
	"github.com/gorilla/websocket"
)

// WebSocket Upgrader，升级 HTTP 连接到 WebSocket
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // 允许所有来源
	},
}

// WebSocket 代理，转发 WebSocket 请求
func websocketProxy(req *restful.Request, resp *restful.Response) {
	// 目标 WebSocket 服务器地址
	fmt.Println(req.Request.URL)
	//targetURL := "ws://localhost:9000/ws"
	targetURL := "wss://localhost:8080" + req.Request.URL.RequestURI()
	fmt.Println("targeturl: ", targetURL)
	//targetURL := "wss://localhost:8080?clientid=guoj111&protocol=ws&address=ws://127.0.0.1:9090/ws"
	// 解析目标 URL
	u, err := url.Parse(targetURL)
	if err != nil {

		http.Error(resp, "Invalid WebSocket URL", http.StatusInternalServerError)
		return
	}

	// 获取客户端的 WebSocket 连接
	clientConn, err := upgrader.Upgrade(resp.ResponseWriter, req.Request, nil)
	if err != nil {
		fmt.Println(err)
		http.Error(resp, "Failed to upgrade connection", http.StatusInternalServerError)
		return
	}
	defer clientConn.Close()
	ca := "D:/workspace/go/src/test/multi-cluster/cert/ca_cert.pem"
	cert := "D:/workspace/go/src/test/multi-cluster/cert/client_cert.pem"
	key := "D:/workspace/go/src/test/multi-cluster/cert/client_key.pem"
	// ca := "D:/workspace/go/src/test/multi-cluster/cert/ca_cert.pem"
	// cert := "D:/workspace/go/src/test/multi-cluster/cert/server_cert.pem"
	// key := "D:/workspace/go/src/test/multi-cluster/cert/server_key.pem"
	// TLS 配置（双向认证）
	// tlsConfig := &tls.Config{
	// 	InsecureSkipVerify: false, // 设为 true 会跳过证书检查（不推荐）
	// 	Certificates:       []tls.Certificate{loadClientCert()},
	// }

	// 连接目标 WebSocket 服务器
	// dialer := websocket.Dialer{TLSClientConfig: tlsConfig}
	dialer := websocket.Dialer{}
	caCert, err := ioutil.ReadFile(ca)
	if err != nil {
		fmt.Println(err)
		http.Error(resp, fmt.Errorf("failed to read CA cert: %v", err).Error(), http.StatusInternalServerError)

		return
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// load client certificate and key
	clientCert, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		fmt.Println(err)
		http.Error(resp, fmt.Errorf("failed to load client cert/key: %v", err).Error(), http.StatusInternalServerError)
		return
	}

	// configure TLS
	tlsConfig := &tls.Config{
		RootCAs:      caCertPool,                    // CA certificate
		Certificates: []tls.Certificate{clientCert}, // client certificate
	}
	dialer.TLSClientConfig = tlsConfig

	serverConn, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		fmt.Println(err)
		http.Error(resp, "Failed to connect to WebSocket server", http.StatusInternalServerError)
		return
	}
	defer serverConn.Close()
	errChan := make(chan error)

	// **启动双向数据转发**
	go func() {
		errChan <- copyWebSocketMessages(clientConn, serverConn) // 客户端 -> 服务器
	}()
	go func() {
		errChan <- copyWebSocketMessages(serverConn, clientConn) // 服务器 -> 客户端
	}()
	select {
	case err := <-errChan:
		fmt.Println(err)
		return
	}
}

// 复制 WebSocket 消息
func copyWebSocketMessages(src, dest *websocket.Conn) error {
	for {
		msgType, msg, err := src.ReadMessage()
		if err != nil {
			log.Println("Read error:", err)
			return err
		}

		// 转发消息
		err = dest.WriteMessage(msgType, msg)
		if err != nil {
			log.Println("Write error:", err)
			return err
		}
	}
}

// 加载 TLS 证书（用于双向认证）
func loadClientCert() tls.Certificate {
	cert, err := tls.LoadX509KeyPair("client.crt", "client.key")
	if err != nil {
		log.Fatal("Failed to load client certificate:", err)
	}
	return cert
}

func main() {
	// 创建 WebService
	ws := new(restful.WebService)
	ws.Path("/proxy").
		Consumes(restful.MIME_JSON).
		Produces(restful.MIME_JSON).
		Route(ws.GET("").To(websocketProxy)) // WebSocket 代理

	// 启动服务器
	restful.Add(ws)
	fmt.Println("Listening on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
