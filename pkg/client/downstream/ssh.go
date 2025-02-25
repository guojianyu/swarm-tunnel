package downstream

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"time"

	tunnelPkg "github.com/guojianyu/swarm-tunnel/pkg"

	"golang.org/x/crypto/ssh"
)

type SSHClient struct {
	client  *ssh.Client
	session *ssh.Session
	stdin   io.WriteCloser
	stdout  io.Reader
}

// connectSSH establishes an SSH connection and returns the client and session
func connectSSH(config *tunnelPkg.SSH) (*ssh.Client, *ssh.Session, error) {
	var auth ssh.AuthMethod
	if len(config.Password) == 0 {
		privateKeyPath := "root/.ssh/id_rsa"
		// read privatekey
		key, err := ioutil.ReadFile(privateKeyPath)
		if err != nil {
			return nil, nil, fmt.Errorf("Error reading  ssh privatekey: %v", err)
		}
		// parse privatekey
		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			return nil, nil, fmt.Errorf("Error parseing  ssh privatekey: %v", err)
		}
		auth = ssh.PublicKeys(signer)
		config.User = "root"
	} else {
		auth = ssh.Password(config.Password)
	}
	sshConfig := &ssh.ClientConfig{
		User: config.User,
		Auth: []ssh.AuthMethod{
			//ssh.Password(config.Password),
			auth,
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

func NewSSHClient(tunnelMessage *tunnelPkg.TunnelMessage) (*SSHClient, error) {
	log.Println("ssh config:", tunnelMessage.SSH)
	var err error
	sshclient := &SSHClient{}
	sshclient.client, sshclient.session, err = connectSSH(tunnelMessage.SSH)
	if err != nil {
		log.Println("SSH Connection Error:", err)
		return nil, err
	}

	//defer client.Close()
	//defer session.Close()

	// Create pipes for SSH session input/output
	sshclient.stdin, err = sshclient.session.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("Error creating StdinPipe:%v", err)
	}
	sshclient.stdout, err = sshclient.session.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("Error creating StdoutPipe:%v", err)
	}

	// Request pseudo-terminal for interactive SSH session
	if err := sshclient.session.RequestPty("xterm", 80, 80, ssh.TerminalModes{}); err != nil {
		log.Println("Error requesting PTY:", err)
		return nil, fmt.Errorf("Error requesting PTY:%v", err)
	}

	// Start remote shell
	if err := sshclient.session.Shell(); err != nil {
		return nil, fmt.Errorf("Error starting shell: %v", err)
	}
	return sshclient, nil
}

func (sshclient *SSHClient) Receive(data []byte) error {
	_, err := sshclient.stdin.Write(data)
	if err != nil {
		log.Println("Error writing to SSH stdin:", err)
	}
	return err
}
func (sshclient *SSHClient) Send() (data []byte, err error) {
	buffer := make([]byte, 1024)
	n, err := sshclient.stdout.Read(buffer)
	if err != nil {
		log.Println("Error reading from SSH stdout:", err)
		return nil, err
	}
	return buffer[:n], nil
}

func (sshclient *SSHClient) Close() error {
	sshclient.session.Close()
	sshclient.client.Close()
	return nil
}
