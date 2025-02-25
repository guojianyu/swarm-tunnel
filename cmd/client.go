package main

import (
	"flag"

	"github.com/guojianyu/swarm-tunnel/pkg/client"

	"k8s.io/klog"
)

func main() {
	klog.InitFlags(nil)
	flag.Set("v", "3")
	flag.Parse()
	addr := "ws://localhost:8080/register"
	clientId := "guojy1"
	tunnelAgent := client.NewTunnelAgent(addr, clientId)
	// {
	// 	addr := "wss://localhost:8080/register"
	// 	ca := "D:/workspace/go/src/test/multi-cluster/cert/ca_cert.pem"
	// 	cert := "D:/workspace/go/src/test/multi-cluster/cert/client_cert.pem"
	// 	key := "D:/workspace/go/src/test/multi-cluster/cert/client_key.pem"
	// 	tunnelAgent.WithTLS(ca, cert, key)

	// }
	tunnelAgent.Run()
}
