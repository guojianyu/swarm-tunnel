package main

import (
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/gorilla/websocket"
)

// WebSocket 升级器
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // 允许所有跨域请求，生产环境需加强安全性
	},
}

func fileReceiverHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("WebSocket upgrade failed:", err)
		return
	}
	defer conn.Close()

	// 创建目标文件
	outputFile, err := os.Create("./received_file")
	if err != nil {
		fmt.Println("Failed to create file:", err)
		return
	}
	defer outputFile.Close()

	fmt.Println("Receiving file through WebSocket...")

	// 持续接收 WebSocket 数据并写入文件
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			if err == io.EOF {
				fmt.Println("File received successfully")
				break
			}
			fmt.Println("WebSocket read error:", err)
			return
		}
		_, writeErr := outputFile.Write(data)
		if writeErr != nil {
			fmt.Println("Failed to write to file:", writeErr)
			return
		}
	}

	fmt.Println("File received and saved successfully")
}

func main() {
	http.HandleFunc("/upload", fileReceiverHandler)
	fmt.Println("Server B listening on :9000")
	http.ListenAndServe(":9000", nil)
}
