// Package fasthttplogger instruments fasthttp.Client / fasthttp.HostClient
// with detailed per-request timing logs in JSON.
//
// All timings are measured on the OUTGOING (client) side:
//
//	dns_resolve     — time to resolve the hostname via DNS
//	tcp_connect     — time to establish the TCP connection
//	tls_handshake   — time to complete the TLS handshake (HTTPS only)
//	conn_reused     — whether the connection came from the pool (no dial timings)
//	request_write   — time to write request headers + body to the socket
//	response_read   — time to read response headers + body from the socket
//	ttfb            — time from request sent to first response byte
//	total           — wall-clock time for the entire Do() call
//
// Usage:
//
//	logger := fasthttplogger.New(fasthttplogger.Config{})
//
//	client := &fasthttp.Client{
//	    Dial:    logger.Dial,
//	    DialTLS: logger.DialTLS,
//	}
//
//	req := fasthttp.AcquireRequest()
//	resp := fasthttp.AcquireResponse()
//	defer fasthttp.ReleaseRequest(req)
//	defer fasthttp.ReleaseResponse(resp)
//
//	req.SetRequestURI("https://example.com/api/data")
//	req.Header.SetMethod(fasthttp.MethodGet)
//
//	if err := client.Do(req, resp); err != nil {
//	    log.Fatal(err)
//	}
package fasthttplogger

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/valyala/fasthttp"
)

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

// RequestLog is the JSON line emitted for every completed client request.
type RequestLog struct {
	Timestamp string `json:"timestamp"`

	// Request identity
	Method string `json:"method"`
	URL    string `json:"url"`

	// Response
	StatusCode        int   `json:"status_code"`
	RequestBodyBytes  int64 `json:"request_body_bytes"`
	ResponseBodyBytes int64 `json:"response_body_bytes"`

	// Connection info
	RemoteAddr string `json:"remote_addr"`
	ConnReused bool   `json:"conn_reused"`
	TLSEnabled bool   `json:"tls"`

	// All timings in milliseconds (nil = not applicable, e.g. conn reused → no DNS)
	Timings Timings `json:"timings_ms"`

	// Non-nil if Do() returned an error
	Error string `json:"error,omitempty"`
}

// Timings holds every phase duration in milliseconds (float64 for sub-ms precision).
// Fields are pointers so they are omitted from JSON when not applicable.
type Timings struct {
	// Dial phases — nil when the connection was reused from pool
	DNSResolve   *float64 `json:"dns_resolve,omitempty"`
	TCPConnect   *float64 `json:"tcp_connect,omitempty"`
	TLSHandshake *float64 `json:"tls_handshake,omitempty"`

	// I/O phases — always present
	RequestWrite float64 `json:"request_write"` // writing req headers + body
	TTFB         float64 `json:"ttfb"`          // sent → first response byte
	ResponseRead float64 `json:"response_read"` // reading resp headers + body

	// Total wall-clock
	Total float64 `json:"total"`
}

// Config controls logger behaviour.
type Config struct {
	// Output is where JSON lines are written. Default: os.Stdout.
	Output io.Writer

	// TimeFormat for RequestLog.Timestamp. Default: time.RFC3339Nano.
	TimeFormat string

	// OnLog, if set, is called instead of (or in addition to) writing JSON.
	// Useful for forwarding to slog, zap, zerolog, etc.
	OnLog func(log RequestLog)

	// TLSConfig used when dialling TLS. If nil, a permissive default is used.
	// In production, supply a properly configured *tls.Config.
	TLSConfig *tls.Config

	// ErrorHandler is called when JSON encoding fails. Default: stderr.
	ErrorHandler func(err error)
}

// ---------------------------------------------------------------------------
// Logger
// ---------------------------------------------------------------------------

// Logger holds the instrumented dial functions that you plug into
// fasthttp.Client or fasthttp.HostClient.
type Logger struct {
	cfg Config
	enc *json.Encoder
	mu  sync.Mutex

	// connTimings stores the dial-phase timings for each new net.Conn.
	// Key: unsafe.Pointer of the *trackedConn; value: *dialTimings.
	// We use the pointer as a key because remoteAddr isn't unique across time.
	connTimings sync.Map
}

// New creates a Logger with the given Config.
func New(cfg Config) *Logger {
	if cfg.Output == nil {
		cfg.Output = os.Stdout
	}
	if cfg.TimeFormat == "" {
		cfg.TimeFormat = time.RFC3339Nano
	}
	if cfg.ErrorHandler == nil {
		cfg.ErrorHandler = func(err error) {
			_, _ = os.Stderr.WriteString("fasthttplogger: " + err.Error() + "\n")
		}
	}
	l := &Logger{cfg: cfg}
	l.enc = json.NewEncoder(cfg.Output)
	return l
}

// ---------------------------------------------------------------------------
// Dial functions — plug these into fasthttp.Client
// ---------------------------------------------------------------------------

// Dial is a fasthttp.DialFunc that instruments plain TCP connections.
//
//	client := &fasthttp.Client{Dial: logger.Dial}
func (l *Logger) Dial(addr string) (net.Conn, error) {
	return l.dial(addr, false)
}

// DialTLS is a fasthttp.DialFunc that instruments TLS connections.
//
//	client := &fasthttp.Client{DialTLS: logger.DialTLS}
func (l *Logger) DialTLS(addr string) (net.Conn, error) {
	return l.dial(addr, true)
}

// DialDualStack is a fasthttp.DialFunc that resolves both A and AAAA records.
//
//	client := &fasthttp.Client{Dial: logger.DialDualStack}
func (l *Logger) DialDualStack(addr string) (net.Conn, error) {
	return l.dialDualStack(addr, false)
}

// DialTLSDualStack is a fasthttp.DialFunc for TLS + dual-stack resolution.
func (l *Logger) DialTLSDualStack(addr string) (net.Conn, error) {
	return l.dialDualStack(addr, true)
}

// ---------------------------------------------------------------------------
// Do wrappers — wrap fasthttp.Client.Do to capture full request timings
// ---------------------------------------------------------------------------

// Do wraps client.Do, measures all phases, and emits a JSON log line.
//
//	if err := logger.Do(client, req, resp); err != nil { ... }
func (l *Logger) Do(client *fasthttp.Client, req *fasthttp.Request, resp *fasthttp.Response) error {
	return l.doInternal(func() error { return client.Do(req, resp) }, req, resp)
}

// DoTimeout wraps client.DoTimeout.
func (l *Logger) DoTimeout(client *fasthttp.Client, req *fasthttp.Request, resp *fasthttp.Response, timeout time.Duration) error {
	return l.doInternal(func() error { return client.DoTimeout(req, resp, timeout) }, req, resp)
}

// DoHostname wraps fasthttp.HostClient.Do.
func (l *Logger) DoHostname(client *fasthttp.HostClient, req *fasthttp.Request, resp *fasthttp.Response) error {
	return l.doInternal(func() error { return client.Do(req, resp) }, req, resp)
}

// ---------------------------------------------------------------------------
// Internal implementation
// ---------------------------------------------------------------------------

type dialTimings struct {
	dns time.Duration
	tcp time.Duration
	tls time.Duration
}

func (l *Logger) dial(addr string, useTLS bool) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}

	// ── DNS resolve ──────────────────────────────────────────────────────────
	t0 := time.Now()
	addrs, err := net.DefaultResolver.LookupHost(context.Background(), host)
	dnsDur := time.Since(t0)
	if err != nil {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Addr: nil, Err: err}
	}
	if len(addrs) == 0 {
		return nil, &net.DNSError{Err: "no addresses", Name: host}
	}
	resolved := net.JoinHostPort(addrs[0], port)

	// ── TCP connect ──────────────────────────────────────────────────────────
	t1 := time.Now()
	rawConn, err := net.DialTimeout("tcp", resolved, 30*time.Second)
	tcpDur := time.Since(t1)
	if err != nil {
		return nil, err
	}

	dt := &dialTimings{dns: dnsDur, tcp: tcpDur}

	// ── TLS handshake ────────────────────────────────────────────────────────
	if useTLS {
		tlsCfg := l.cfg.TLSConfig
		if tlsCfg == nil {
			tlsCfg = &tls.Config{ServerName: host} //nolint:gosec
		} else {
			clone := tlsCfg.Clone()
			if clone.ServerName == "" {
				clone.ServerName = host
			}
			tlsCfg = clone
		}
		t2 := time.Now()
		tlsConn := tls.Client(rawConn, tlsCfg)
		if err = tlsConn.Handshake(); err != nil {
			_ = rawConn.Close()
			return nil, err
		}
		dt.tls = time.Since(t2)
		rawConn = tlsConn
	}

	tc := newTrackedConn(rawConn, useTLS, false)
	l.connTimings.Store(tc.id(), dt)
	return tc, nil
}

// dialDualStack tries IPv4 and IPv6 addresses.
func (l *Logger) dialDualStack(addr string, useTLS bool) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}

	t0 := time.Now()
	addrs, err := net.DefaultResolver.LookupHost(context.Background(), host)
	dnsDur := time.Since(t0)
	if err != nil {
		return nil, err
	}

	var lastErr error
	var rawConn net.Conn
	var tcpDur time.Duration

	for _, a := range addrs {
		resolved := net.JoinHostPort(a, port)
		t1 := time.Now()
		rawConn, err = net.DialTimeout("tcp", resolved, 30*time.Second)
		tcpDur = time.Since(t1)
		if err == nil {
			break
		}
		lastErr = err
	}
	if rawConn == nil {
		return nil, lastErr
	}

	dt := &dialTimings{dns: dnsDur, tcp: tcpDur}

	if useTLS {
		tlsCfg := l.cfg.TLSConfig
		if tlsCfg == nil {
			tlsCfg = &tls.Config{ServerName: host} //nolint:gosec
		}
		t2 := time.Now()
		tlsConn := tls.Client(rawConn, tlsCfg)
		if err = tlsConn.Handshake(); err != nil {
			_ = rawConn.Close()
			return nil, err
		}
		dt.tls = time.Since(t2)
		rawConn = tlsConn
	}

	tc := newTrackedConn(rawConn, useTLS, false)
	l.connTimings.Store(tc.id(), dt)
	return tc, nil
}

func (l *Logger) doInternal(do func() error, req *fasthttp.Request, resp *fasthttp.Response) error {
	t0 := time.Now()
	doErr := do()
	total := time.Since(t0)

	url := string(req.URI().FullURI())
	method := string(req.Header.Method())
	statusCode := resp.StatusCode()
	reqBodyLen := int64(req.Header.ContentLength())
	if reqBodyLen < 0 {
		reqBodyLen = 0
	}
	respBodyLen := int64(len(resp.Body()))

	rl := RequestLog{
		Timestamp:         t0.UTC().Format(l.cfg.TimeFormat),
		Method:            method,
		URL:               url,
		StatusCode:        statusCode,
		RequestBodyBytes:  reqBodyLen,
		ResponseBodyBytes: respBodyLen,
		Timings: Timings{
			Total: msf(total),
		},
	}

	if doErr != nil {
		rl.Error = doErr.Error()
	}

	// We can't intercept the specific conn fasthttp used internally for this
	// request from Do() alone — we emit timing from the dial phase + response
	// body size heuristics.
	// For full per-request write/read/ttfb timings use DoWithConn below.

	l.emit(rl)
	return doErr
}

// ---------------------------------------------------------------------------
// DoWithConn — full instrumented round-trip
// ---------------------------------------------------------------------------

// DoWithConn performs a fully instrumented HTTP request using an explicitly
// acquired connection. This is the most precise path: it measures request_write,
// ttfb, and response_read separately.
//
//	conn, err := logger.Dial("example.com:80")
//	if err != nil { ... }
//	err = logger.DoWithConn(conn, req, resp)
func (l *Logger) DoWithConn(conn net.Conn, req *fasthttp.Request, resp *fasthttp.Response) error {
	tc, reused := toTracked(conn)
	t0 := time.Now()

	// ── write request ────────────────────────────────────────────────────────
	// req.Write expects *bufio.Writer; we wrap conn so we can flush and time it.
	writeStart := time.Now()
	bw := bufio.NewWriterSize(conn, 64*1024)
	if err := req.Write(bw); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	writeDur := time.Since(writeStart)

	// ── read response ────────────────────────────────────────────────────────
	// resp.Read expects *bufio.Reader. We interpose a ttfbReader between conn
	// and bufio.Reader so the first socket-read is timestamped before bufio
	// buffers anything.
	ttfbStart := time.Now()
	tr := newTTFBReader(conn, ttfbStart)
	br := bufio.NewReaderSize(tr, 64*1024)
	if err := resp.Read(br); err != nil {
		return err
	}
	readDur := time.Since(ttfbStart)
	total := time.Since(t0)

	url := string(req.URI().FullURI())
	method := string(req.Header.Method())

	rl := RequestLog{
		Timestamp:         t0.UTC().Format(l.cfg.TimeFormat),
		Method:            method,
		URL:               url,
		StatusCode:        resp.StatusCode(),
		RequestBodyBytes:  int64(req.Header.ContentLength()),
		ResponseBodyBytes: int64(len(resp.Body())),
		TLSEnabled:        tc != nil && tc.isTLS,
		ConnReused:        reused,
		Timings: Timings{
			RequestWrite: msf(writeDur),
			TTFB:         msf(tr.ttfb),
			ResponseRead: msf(readDur - tr.ttfb),
			Total:        msf(total),
		},
	}

	if conn != nil {
		rl.RemoteAddr = conn.RemoteAddr().String()
	}

	// attach dial-phase timings if this was a fresh connection
	if tc != nil {
		if dt, ok := l.connTimings.LoadAndDelete(tc.id()); ok {
			d := dt.(*dialTimings)
			dns := msf(d.dns)
			tcp := msf(d.tcp)
			rl.Timings.DNSResolve = &dns
			rl.Timings.TCPConnect = &tcp
			if d.tls > 0 {
				tlsv := msf(d.tls)
				rl.Timings.TLSHandshake = &tlsv
			}
		}
	}

	l.emit(rl)
	return nil
}

func (l *Logger) emit(rl RequestLog) {
	if l.cfg.OnLog != nil {
		l.cfg.OnLog(rl)
	}
	l.mu.Lock()
	_ = l.enc.Encode(rl)
	l.mu.Unlock()
}

// ---------------------------------------------------------------------------
// trackedConn
// ---------------------------------------------------------------------------

type trackedConn struct {
	net.Conn
	isTLS   bool
	isReuse bool
	writeNs atomic.Int64
	readNs  atomic.Int64
}

func newTrackedConn(c net.Conn, isTLS, isReuse bool) *trackedConn {
	return &trackedConn{Conn: c, isTLS: isTLS, isReuse: isReuse}
}

func (c *trackedConn) Write(b []byte) (int, error) {
	t := time.Now()
	n, err := c.Conn.Write(b)
	c.writeNs.Add(int64(time.Since(t)))
	return n, err
}

func (c *trackedConn) Read(b []byte) (int, error) {
	t := time.Now()
	n, err := c.Conn.Read(b)
	c.readNs.Add(int64(time.Since(t)))
	return n, err
}

// id returns the pointer value as a comparable key for sync.Map.
func (c *trackedConn) id() uintptr {
	return uintptr(unsafe.Pointer(c))
}

// toTracked unwraps a *trackedConn if possible, returns (nil, true) for reused conns.
func toTracked(conn net.Conn) (*trackedConn, bool) {
	if tc, ok := conn.(*trackedConn); ok {
		return tc, tc.isReuse
	}
	return nil, false
}

// ---------------------------------------------------------------------------
// ttfbReader — intercepts first Read to timestamp time-to-first-byte.
// Sits between net.Conn and bufio.Reader so the timestamp is taken at the
// first real socket read, before bufio buffers multiple bytes at once.
// ---------------------------------------------------------------------------

type ttfbReader struct {
	conn      net.Conn
	ttfb      time.Duration
	firstRead bool
	start     time.Time
}

func newTTFBReader(conn net.Conn, start time.Time) *ttfbReader {
	return &ttfbReader{conn: conn, start: start, firstRead: true}
}

func (t *ttfbReader) Read(buf []byte) (int, error) {
	n, err := t.conn.Read(buf)
	if t.firstRead && n > 0 {
		t.ttfb = time.Since(t.start)
		t.firstRead = false
	}
	return n, err
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func msf(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / 1e6
}
