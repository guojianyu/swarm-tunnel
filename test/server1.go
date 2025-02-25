package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/crypto/ssh"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// SSHConfig contains SSH server connection details
type SSHConfig struct {
	Host     string
	Port     string
	User     string
	Password string
}

// ResizeMessage 表示前端发送的终端大小调整消息
type ResizeMessage struct {
	Width  int `json:"width"`
	Height int `json:"height"`
}

// handleResize 动态调整终端大小
func handleResize(session *ssh.Session, message []byte) error {
	var resize ResizeMessage
	if err := json.Unmarshal(message, &resize); err != nil {
		return err
	}
	return session.WindowChange(resize.Height, resize.Width)
}

// handleWebSocket manages WebSocket connections and SSH interactions
func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket Upgrade Error:", err)
		return
	}
	defer conn.Close()

	sshConfig := SSHConfig{
		Host:     "192.168.2.204", // Replace with actual SSH server
		Port:     "22",
		User:     "root",
		Password: "1",
	}

	client, session, err := connectSSH(sshConfig)
	if err != nil {
		log.Println("SSH Connection Error:", err)
		conn.WriteMessage(websocket.TextMessage, []byte("SSH connection failed: "+err.Error()))
		return
	}
	defer client.Close()
	defer session.Close()

	// Create pipes for SSH session input/output
	stdin, err := session.StdinPipe()
	if err != nil {
		log.Println("Error creating StdinPipe:", err)
		return
	}
	stdout, err := session.StdoutPipe()
	if err != nil {
		log.Println("Error creating StdoutPipe:", err)
		return
	}

	// Request pseudo-terminal for interactive SSH session
	if err := session.RequestPty("xterm", 80, 80, ssh.TerminalModes{}); err != nil {
		log.Println("Error requesting PTY:", err)
		return
	}

	// Start remote shell
	if err := session.Shell(); err != nil {
		log.Println("Error starting shell:", err)
		return
	}

	// Handle WebSocket to SSH input
	go func() {
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Println("WebSocket Read Error:", err)
				return
			}

			// 动态调整终端大小
			if string(message[:8]) == "RESIZE: " {
				handleResize(session, message[8:])
				continue
			}
			_, err = stdin.Write(message)
			if err != nil {
				log.Println("Error writing to SSH stdin:", err)
				return
			}
		}
	}()

	// Handle SSH to WebSocket output
	buffer := make([]byte, 1024)
	for {
		n, err := stdout.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println("Error reading from SSH stdout:", err)
			return
		}
		err = conn.WriteMessage(websocket.TextMessage, buffer[:n])
		if err != nil {
			log.Println("WebSocket Write Error:", err)
			return
		}
	}
}

// connectSSH establishes an SSH connection and returns the client and session
func connectSSH(config SSHConfig) (*ssh.Client, *ssh.Session, error) {
	sshConfig := &ssh.ClientConfig{
		User: config.User,
		Auth: []ssh.AuthMethod{
			ssh.Password(config.Password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         5 * time.Second,
	}
	address := config.Host + ":" + config.Port
	client, err := ssh.Dial("tcp", address, sshConfig)
	if err != nil {
		return nil, nil, err
	}

	session, err := client.NewSession()
	if err != nil {
		client.Close()
		return nil, nil, err
	}

	return client, session, nil
}

func main() {
	http.HandleFunc("/ws", handleWebSocket)

	// Gracefully handle server shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt)
		<-sigCh
		log.Println("Shutting down server...")
		os.Exit(0)
	}()

	log.Println("WebSocket server running on ws://localhost:8060/ws")
	log.Fatal(http.ListenAndServe(":8060", nil))
}
