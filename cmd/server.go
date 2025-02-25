package main

import (
	"flag"
	"fmt"
	"swarm-tunnel/pkg/server"

	"k8s.io/klog"
)

func main() {
	klog.InitFlags(nil)
	flag.Set("v", "3")
	flag.Parse()
	tunnelServer := server.NewTunnelServer(":8080")
	tunnelServer.SetRegisterClientCallback(func(clientID string) {
		fmt.Printf("dynamic callback：client[%s] register!\n", clientID)
	})
	tunnelServer.SetUnregisterClientCallback(func(clientID string) {
		fmt.Printf("dynamic callback：client[%s] unregister!\n", clientID)
	})
	tunnelServer.SetRegisterSessionCallback(func(sessionID string) {
		fmt.Printf("dynamic callback：session[%s] register!\n", sessionID)
	})
	tunnelServer.SetUnregisterSessionCallback(func(sessionID string) {
		fmt.Printf("dynamic callback：session[%s] unregister!\n", sessionID)
	})

	// {
	// 	ca := "D:/workspace/go/src/test/multi-cluster/cert/ca_cert.pem"
	// 	cert := "D:/workspace/go/src/test/multi-cluster/cert/server_cert.pem"
	// 	key := "D:/workspace/go/src/test/multi-cluster/cert/server_key.pem"
	// 	tunnelServer.WithTLS(ca, cert, key)

	// }

	tunnelServer.Run()
}
