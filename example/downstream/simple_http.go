package main

import (
	"fmt"
	"log"
	"net/http"
)

func main() {
	// 处理根路径请求
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Welcome to the Go HTTP Server!")
	})

	// 处理 `/hello` 路径请求
	http.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		// 从查询参数中获取 `name` 值
		name := r.URL.Query().Get("name")
		if name == "" {
			name = "World"
		}
		fmt.Fprintf(w, "Hello, %s!", name)
	})

	// 启动服务器
	port := ":8060"
	fmt.Printf("Starting server on port %s\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
