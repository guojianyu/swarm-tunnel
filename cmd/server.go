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
package main

import (
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/guojianyu/swarm-tunnel/pkg/server"

	"k8s.io/klog"
)

func main() {
	klog.InitFlags(nil)
	flag.Set("v", "4")
	flag.Parse()
	tunnelServer := server.NewTunnelServer(":8080")

	tunnelServer.SetRegisterClientHandle(func(clientID string, r *http.Request) error {
		fmt.Printf("dynamic Handle[%s]\n", clientID)
		return nil
	})

	tunnelServer.SetRegisterdClientCallback(func(clientID string) {
		fmt.Printf("dynamic callback：client[%s] register!\n", clientID)
	})
	tunnelServer.SetUnregisterdClientCallback(func(clientID string) {
		fmt.Printf("dynamic callback：client[%s] unregister!\n", clientID)
	})
	tunnelServer.SetRegisterdSessionCallback(func(sessionID string) {
		fmt.Printf("dynamic callback：session[%s] register!\n", sessionID)
	})
	tunnelServer.SetUnregisterdSessionCallback(func(sessionID string) {
		fmt.Printf("dynamic callback：session[%s] unregister!\n", sessionID)
	})

	// {
	// 	ca := "D:/workspace/go/src/test/multi-cluster/cert/ca_cert.pem"
	// 	cert := "D:/workspace/go/src/test/multi-cluster/cert/server_cert.pem"
	// 	key := "D:/workspace/go/src/test/multi-cluster/cert/server_key.pem"
	// 	tunnelServer.WithTLS(ca, cert, key)

	// }
	go func() {
		time.Sleep(10 * time.Second)
		err := tunnelServer.ManualDisconnenctClient("guojy")
		fmt.Printf("Disconnenct [%v]client! ,%v\n", "guojy", err)

	}()
	tunnelServer.Run()
}
