package fasthttplogger

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/valyala/fasthttp"
)

func TestLogger_Do(t *testing.T) {
	// Create a test server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond) // Simulate work
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello world"))
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
	req.Header.SetMethod(fasthttp.MethodGet)

	err := logger.Do(client, req, resp)
	if err != nil {
		t.Fatalf("logger.Do failed: %v", err)
	}

	if resp.StatusCode() != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode())
	}

	// Verify log output
	var rl RequestLog
	err = json.Unmarshal(logOutput.Bytes(), &rl)
	if err != nil {
		t.Fatalf("failed to parse log output: %v", err)
	}

	if rl.Method != fasthttp.MethodGet {
		t.Errorf("expected method GET, got %s", rl.Method)
	}

	if rl.URL != ts.URL && rl.URL != ts.URL+"/"+"" {
		t.Errorf("expected URL %s, got %s", ts.URL, rl.URL)
	}

	if rl.StatusCode != http.StatusOK {
		t.Errorf("expected status code 200, got %d", rl.StatusCode)
	}

	if rl.Timings.Total <= 0 {
		t.Error("expected total timing to be greater than 0")
	}
}

func TestLogger_DoWithConn(t *testing.T) {
	// Create a test server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello world"))
	}))
	defer ts.Close()

	var logOutput bytes.Buffer
	logger := New(Config{
		Output: &logOutput,
	})

	// We need a real connection to the test server
	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial test server: %v", err)
	}
	defer conn.Close()

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	// Set up request for HTTP/1.1
	req.SetRequestURI(ts.URL)
	req.Header.SetMethod(fasthttp.MethodGet)
	req.Header.Set("Host", "localhost")

	err = logger.DoWithConn(conn, req, resp)
	if err != nil {
		t.Fatalf("logger.DoWithConn failed: %v", err)
	}

	// Verify log output
	var rl RequestLog
	err = json.Unmarshal(logOutput.Bytes(), &rl)
	if err != nil {
		t.Fatalf("failed to parse log output: %v", err)
	}

	if rl.StatusCode != http.StatusOK {
		t.Errorf("expected status code 200, got %d", rl.StatusCode)
	}

	if rl.Timings.Total <= 0 {
		t.Error("expected total timing to be greater than 0")
	}

	if rl.Timings.RequestWrite <= 0 || rl.Timings.ResponseRead <= 0 {
		t.Errorf("expected non-zero I/O timings, got write=%f, read=%f",
			rl.Timings.RequestWrite, rl.Timings.ResponseRead)
	}
}

func TestLogger_OnLog(t *testing.T) {
	var capturedLog *RequestLog
	logger := New(Config{
		OnLog: func(log RequestLog) {
			capturedLog = &log
		},
	})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	client := &fasthttp.Client{
		Dial: logger.Dial,
	}

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(ts.URL)

	_ = logger.Do(client, req, resp)

	if capturedLog == nil {
		t.Fatal("OnLog callback was not called")
	}

	if capturedLog.StatusCode != http.StatusOK {
		t.Errorf("expected status 200 in OnLog, got %d", capturedLog.StatusCode)
	}
}
