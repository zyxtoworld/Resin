package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestPumpPreparedTunnelReader_FallsBackToFullCloseWhenHalfCloseUnavailable(t *testing.T) {
	clientBase, clientPeer := net.Pipe()
	upstreamBase, upstreamPeer := net.Pipe()
	defer clientPeer.Close()
	defer upstreamPeer.Close()

	clientConn := &connCloseNotifier{
		Conn: clientBase,
		sink: newCountingConnTestSink(),
	}
	upstreamConn := newTLSLatencyConn(newCountingConn(upstreamBase, newCountingConnTestSink()), nil)

	clientPayloadDone := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(clientPeer)
		clientPayloadDone <- data
	}()

	done := make(chan struct{})
	go func() {
		_ = pumpPreparedTunnelReader(
			clientConn,
			clientConn,
			&preparedTunnel{
				upstreamConn: upstreamConn,
				recordResult: func(bool) {},
			},
			tunnelPumpOptions{},
		)
		close(done)
	}()

	go func() {
		_, _ = upstreamPeer.Write([]byte("server-push"))
		_ = upstreamPeer.Close()
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pumpPreparedTunnelReader should fall back to full close when CloseWrite is unavailable")
	}

	select {
	case payload := <-clientPayloadDone:
		if string(payload) != "server-push" {
			t.Fatalf("client payload: got %q, want %q", string(payload), "server-push")
		}
	case <-time.After(time.Second):
		t.Fatal("expected client peer to receive upstream payload and EOF")
	}
}

func TestPumpPreparedTunnelReader_ClientReadResetAfterIngressDoesNotFail(t *testing.T) {
	clientLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen client side: %v", err)
	}
	defer clientLn.Close()

	clientAccepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := clientLn.Accept()
		if acceptErr != nil {
			clientAccepted <- nil
			return
		}
		clientAccepted <- conn
	}()

	clientConn, err := net.Dial("tcp", clientLn.Addr().String())
	if err != nil {
		t.Fatalf("dial client side: %v", err)
	}
	clientTCP, ok := clientConn.(*net.TCPConn)
	if !ok {
		t.Fatalf("client conn type: got %T, want *net.TCPConn", clientConn)
	}
	defer clientTCP.Close()

	proxyClientConn := <-clientAccepted
	if proxyClientConn == nil {
		t.Fatal("accept client side failed")
	}
	defer proxyClientConn.Close()

	upstreamLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen upstream side: %v", err)
	}
	defer upstreamLn.Close()

	upstreamAccepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := upstreamLn.Accept()
		if acceptErr != nil {
			upstreamAccepted <- nil
			return
		}
		upstreamAccepted <- conn
	}()

	upstreamPeer, err := net.Dial("tcp", upstreamLn.Addr().String())
	if err != nil {
		t.Fatalf("dial upstream side: %v", err)
	}
	defer upstreamPeer.Close()

	proxyUpstreamConn := <-upstreamAccepted
	if proxyUpstreamConn == nil {
		t.Fatal("accept upstream side failed")
	}
	defer proxyUpstreamConn.Close()

	resultCh := make(chan tunnelRelayResult, 1)
	go func() {
		resultCh <- pumpPreparedTunnelReader(
			proxyClientConn,
			proxyClientConn,
			&preparedTunnel{
				upstreamConn: proxyUpstreamConn,
				recordResult: func(bool) {},
			},
			tunnelPumpOptions{},
		)
	}()

	const request = "client-hello"
	const response = "server-reply"
	upstreamDone := make(chan error, 1)
	go func() {
		defer close(upstreamDone)
		buf := make([]byte, len(request))
		if _, err := io.ReadFull(upstreamPeer, buf); err != nil {
			upstreamDone <- err
			return
		}
		if string(buf) != request {
			upstreamDone <- io.ErrUnexpectedEOF
			return
		}
		if _, err := upstreamPeer.Write([]byte(response)); err != nil {
			upstreamDone <- err
			return
		}
		_, _ = io.Copy(io.Discard, upstreamPeer)
		upstreamDone <- nil
	}()

	if _, err := clientTCP.Write([]byte(request)); err != nil {
		t.Fatalf("write client request: %v", err)
	}

	respBuf := make([]byte, len(response))
	if _, err := io.ReadFull(clientTCP, respBuf); err != nil {
		t.Fatalf("read client response: %v", err)
	}
	if string(respBuf) != response {
		t.Fatalf("response: got %q, want %q", string(respBuf), response)
	}

	if err := clientTCP.SetLinger(0); err != nil {
		t.Fatalf("set linger: %v", err)
	}
	if err := clientTCP.Close(); err != nil {
		t.Fatalf("close client with reset: %v", err)
	}

	select {
	case result := <-resultCh:
		if !result.netOK {
			t.Fatalf("netOK: got false, want true (stage=%q err=%v)", result.upstreamStage, result.upstreamErr)
		}
		if result.proxyErr != nil {
			t.Fatalf("proxyErr: got %+v, want nil", result.proxyErr)
		}
		if result.upstreamStage != "" {
			t.Fatalf("upstreamStage: got %q, want empty", result.upstreamStage)
		}
		if result.ingressBytes != int64(len(response)) {
			t.Fatalf("ingressBytes: got %d, want %d", result.ingressBytes, len(response))
		}
		if result.egressBytes != int64(len(request)) {
			t.Fatalf("egressBytes: got %d, want %d", result.egressBytes, len(request))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected tunnel relay result")
	}

	select {
	case err := <-upstreamDone:
		if err != nil {
			t.Fatalf("upstream side failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected upstream side to finish")
	}
}

type scriptedTunnelConn struct {
	readFn       func([]byte) (int, error)
	writeFn      func([]byte) (int, error)
	closeWriteFn func() error
}

func (c *scriptedTunnelConn) Read(p []byte) (int, error) {
	return c.readFn(p)
}

func (c *scriptedTunnelConn) Write(p []byte) (int, error) {
	return c.writeFn(p)
}

func (c *scriptedTunnelConn) Close() error { return nil }

func (c *scriptedTunnelConn) CloseWrite() error {
	if c.closeWriteFn != nil {
		return c.closeWriteFn()
	}
	return nil
}

func (c *scriptedTunnelConn) LocalAddr() net.Addr  { return stubAddr("local") }
func (c *scriptedTunnelConn) RemoteAddr() net.Addr { return stubAddr("remote") }

func (c *scriptedTunnelConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedTunnelConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedTunnelConn) SetWriteDeadline(time.Time) error { return nil }

type delayedCancelTunnelContext struct {
	done     chan struct{}
	canceled atomic.Bool
}

func newDelayedCancelTunnelContext() *delayedCancelTunnelContext {
	return &delayedCancelTunnelContext{done: make(chan struct{})}
}

func (c *delayedCancelTunnelContext) requestCancel() {
	c.canceled.Store(true)
}

func (c *delayedCancelTunnelContext) release() {
	close(c.done)
}

func (c *delayedCancelTunnelContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *delayedCancelTunnelContext) Done() <-chan struct{}       { return c.done }
func (c *delayedCancelTunnelContext) Err() error {
	if c.canceled.Load() {
		return context.Canceled
	}
	return nil
}
func (c *delayedCancelTunnelContext) Value(any) any { return nil }

type cancelOnCloseTunnelConn struct {
	*scriptedTunnelConn
	onClose func()
}

func (c *cancelOnCloseTunnelConn) Close() error {
	if c.onClose != nil {
		c.onClose()
	}
	return nil
}

func TestPumpPreparedTunnelReader_CancellationAfterCopiesBeforeMonitorStopIsNotFailure(t *testing.T) {
	ctx := newDelayedCancelTunnelContext()
	defer ctx.release()

	clientConn := &cancelOnCloseTunnelConn{
		scriptedTunnelConn: &scriptedTunnelConn{
			readFn:  func([]byte) (int, error) { return 0, io.EOF },
			writeFn: func(p []byte) (int, error) { return len(p), nil },
		},
		onClose: ctx.requestCancel,
	}
	upstreamConn := &scriptedTunnelConn{
		readFn:  func([]byte) (int, error) { return 0, io.EOF },
		writeFn: func(p []byte) (int, error) { return len(p), nil },
	}

	result := pumpPreparedTunnelReader(
		clientConn,
		clientConn,
		&preparedTunnel{upstreamConn: upstreamConn},
		tunnelPumpOptions{
			ctx:                         ctx,
			requireBidirectionalTraffic: true,
		},
	)
	if !result.canceled {
		t.Fatalf("cancellation after copies was not classified as cancellation: %+v", result)
	}
	if !result.netOK || result.proxyErr != nil || result.upstreamStage != "" {
		t.Fatalf("cancellation after copies became a relay failure: %+v", result)
	}
}

func TestPumpPreparedTunnelReader_ClientReadResetAfterIngressIsOrdered(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("covers the Windows WSA connection-reset error contract")
	}

	const request = "client-hello"
	const response = "server-reply"
	resetErr := &net.OpError{
		Op:  "read",
		Net: "tcp",
		Err: &os.SyscallError{Syscall: "wsarecv", Err: syscall.Errno(10054)},
	}

	allowReset := make(chan struct{})
	ingressDone := make(chan struct{}, 1)
	clientReads := 0
	clientConn := &scriptedTunnelConn{
		readFn: func(p []byte) (int, error) {
			clientReads++
			if clientReads == 1 {
				return copy(p, request), nil
			}
			<-allowReset
			return 0, resetErr
		},
		writeFn: func(p []byte) (int, error) {
			if string(p) != response {
				return 0, io.ErrUnexpectedEOF
			}
			return len(p), nil
		},
		closeWriteFn: func() error {
			ingressDone <- struct{}{}
			return nil
		},
	}

	requestWritten := make(chan struct{}, 1)
	upstreamReads := 0
	upstreamConn := &scriptedTunnelConn{
		readFn: func(p []byte) (int, error) {
			upstreamReads++
			if upstreamReads == 1 {
				<-requestWritten
				return copy(p, response), nil
			}
			return 0, io.EOF
		},
		writeFn: func(p []byte) (int, error) {
			if string(p) != request {
				return 0, io.ErrUnexpectedEOF
			}
			requestWritten <- struct{}{}
			return len(p), nil
		},
	}

	resultCh := make(chan tunnelRelayResult, 1)
	go func() {
		resultCh <- pumpPreparedTunnelReader(
			clientConn,
			clientConn,
			&preparedTunnel{
				upstreamConn: upstreamConn,
				recordResult: func(bool) {},
			},
			tunnelPumpOptions{},
		)
	}()

	select {
	case <-ingressDone:
	case <-time.After(time.Second):
		t.Fatal("expected ingress copy to finish before releasing client reset")
	}
	close(allowReset)

	select {
	case result := <-resultCh:
		if !result.netOK {
			t.Fatalf("netOK: got false, want true (stage=%q err=%v)", result.upstreamStage, result.upstreamErr)
		}
		if result.proxyErr != nil || result.upstreamStage != "" {
			t.Fatalf("unexpected relay error: proxyErr=%+v stage=%q err=%v", result.proxyErr, result.upstreamStage, result.upstreamErr)
		}
		if result.ingressBytes != int64(len(response)) || result.egressBytes != int64(len(request)) {
			t.Fatalf("bytes: ingress=%d egress=%d, want ingress=%d egress=%d", result.ingressBytes, result.egressBytes, len(response), len(request))
		}
	case <-time.After(time.Second):
		t.Fatal("expected ordered tunnel relay result")
	}
}

func TestPumpPreparedTunnelReader_CancellationDoesNotHideRealCopyError(t *testing.T) {
	sentinelErr := errors.New("upstream copy failed before cancellation")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	clientConn := &scriptedTunnelConn{
		readFn:  func([]byte) (int, error) { return 0, sentinelErr },
		writeFn: func(p []byte) (int, error) { return len(p), nil },
	}
	upstreamConn := &scriptedTunnelConn{
		readFn:  func([]byte) (int, error) { return 0, io.EOF },
		writeFn: func(p []byte) (int, error) { return len(p), nil },
	}

	result := pumpPreparedTunnelReader(
		clientConn,
		clientConn,
		&preparedTunnel{upstreamConn: upstreamConn},
		tunnelPumpOptions{ctx: ctx},
	)
	if result.canceled {
		t.Fatal("real copy error was incorrectly classified as cancellation")
	}
	if result.netOK {
		t.Fatal("real copy error was incorrectly classified as network success")
	}
	if result.proxyErr == nil || result.upstreamErr == nil {
		t.Fatalf("real copy error was lost: proxyErr=%v upstreamErr=%v", result.proxyErr, result.upstreamErr)
	}
}
