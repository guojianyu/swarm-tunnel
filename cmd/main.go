package main

import (
	"flag"
	"swarm-tunnel/pkg/server"

	"k8s.io/klog"
)

func main() {
	klog.InitFlags(nil)
	flag.Set("v", "3")
	flag.Parse()
	tunnelServer := server.NewTunnelServer()
	tunnelServer.Run()
}
