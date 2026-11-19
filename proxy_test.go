package confsource_test

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/stretchr/testify/require"
)

// tcpProxy 夹在 confsource 与 etcd 之间的透传代理，能把已建立的连接变成黑洞：
// 两个方向的字节都吞掉、不关 socket，对双方来说就是一条「还在、但再也不通」的连接，用来模拟静默断连
type tcpProxy struct {
	ln       net.Listener
	upstream string
	mu       sync.Mutex
	conns    []*proxyConn
}

type proxyConn struct {
	frozen atomic.Bool
}

func newTCPProxy(t testingT, upstream string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &tcpProxy{ln: ln, upstream: upstream}
	go p.accept()
	t.Cleanup(func() { ln.Close() })
	return p
}

func (p *tcpProxy) addr() string { return "http://" + p.ln.Addr().String() }

func (p *tcpProxy) accept() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		server, err := net.Dial("tcp", p.upstream)
		if err != nil {
			client.Close()
			continue
		}
		pc := &proxyConn{}
		p.mu.Lock()
		p.conns = append(p.conns, pc)
		p.mu.Unlock()
		go pipe(server, client, pc)
		go pipe(client, server, pc)
	}
}

// pipe 单向转发；冻结后读到的字节直接丢弃，任何一端关闭就把两端都关掉
func pipe(dst, src net.Conn, pc *proxyConn) {
	defer dst.Close()
	defer src.Close()
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if err != nil {
			return
		}
		if pc.frozen.Load() {
			continue
		}
		if _, err := dst.Write(buf[:n]); err != nil {
			return
		}
	}
}

// freezeExisting 把当前已建立的连接全部变成黑洞；之后新建的连接正常转发
func (p *tcpProxy) freezeExisting() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pc := range p.conns {
		pc.frozen.Store(true)
	}
}
