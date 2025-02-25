package main

import (
	"fmt"
	"io"
	"net/http"

	"github.com/gorilla/websocket"
)

var wsConn *websocket.Conn

// 初始化 WebSocket 连接到服务器 B
func initWebSocket() {
	var err error
	wsConn, _, err = websocket.DefaultDialer.Dial("ws://localhost:9000/upload", nil)
	if err != nil {
		panic("Failed to connect to WebSocket server B: " + err.Error())
	}
	fmt.Println("Connected to WebSocket server B")
}

// 处理 HTTP 文件上传
func uploadHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseMultipartForm(10 << 30) // 限制最大 10GB
	file, handler, err := r.FormFile("uploadFile")
	if err != nil {
		http.Error(w, "Failed to get file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	fmt.Printf("Uploading file: %s\n", handler.Filename)

	// 逐块读取文件并通过 WebSocket 发送到服务器 B
	buffer := make([]byte, 4096)
	for {
		n, err := file.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			http.Error(w, "File read error", http.StatusInternalServerError)
			return
		}
		wsConn.WriteMessage(websocket.BinaryMessage, buffer[:n])
	}

	fmt.Fprintf(w, "File %s uploaded and forwarded successfully!", handler.Filename)
}

func main() {
	initWebSocket()
	http.HandleFunc("/upload", uploadHandler)
	fmt.Println("Server A listening on :8080")
	http.ListenAndServe(":8080", nil)
}
