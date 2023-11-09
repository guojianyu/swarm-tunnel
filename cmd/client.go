package main

import (
	"flag"
	"swarm-tunnel/pkg/client"

	"k8s.io/klog"
)

func main() {
	klog.InitFlags(nil)
	flag.Set("v", "3")
	flag.Parse()
	tunnelAgent := client.NewTunnelAgent()
	tunnelAgent.Run()
}
