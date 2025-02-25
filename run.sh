docker save -o swarmclient.tar swarm-client:v1.0
docker save -o swarmserver.tar swarm-server:v1.0
ctr -n k8s.io image import swarmclient.tar
ctr -n k8s.io image import swarmserver.tar