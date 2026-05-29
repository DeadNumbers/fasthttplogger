package fasthttplogger

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/valyala/fasthttp"
)

func BenchmarkLogger_Do(b *testing.B) {
	// Setup test server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer ts.Close()

	var logOutput bytes.Buffer
	logger := New(Config{
		Output: &logOutput,
	})

	client := &fasthttp.Client{
		Dial: logger.Dial,
	}

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI(ts.URL)

	for b.Loop() {
		_ = logger.Do(client, req, resp)
	}
}

func BenchmarkClient_DoDirect(b *testing.B) {
	// Setup test server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer ts.Close()

	client := &fasthttp.Client{}

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI(ts.URL)

	for b.Loop() {
		_ = client.Do(req, resp)
	}
}

func BenchmarkLogger_DoWithConn(b *testing.B) {
	// Setup test server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer ts.Close()

	var logOutput bytes.Buffer
	logger := New(Config{
		Output: &logOutput,
	})

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI(ts.URL)

	for b.Loop() {
		// In a real benchmark, we'd reuse connections, but for simplicity
		// we dial every time to measure the full overhead including dial.
		conn, err := logger.Dial(ts.Listener.Addr().String())
		if err != nil {
			break
		}
		_ = logger.DoWithConn(conn, req, resp)
		conn.Close()
	}
}
