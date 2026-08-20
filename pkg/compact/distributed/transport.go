// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"

	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/runutil"
)

// API paths workers use to talk to the manager. They are served on the
// compactor's existing HTTP server, which every Thanos component already runs.
const (
	APIPrefix     = "/api/v1/compact"
	LeasePath     = APIPrefix + "/lease"
	HeartbeatPath = APIPrefix + "/heartbeat"
	ResultPath    = APIPrefix + "/result"
)

// RegisterServer mounts the manager's task API on a router.
func RegisterServer(mux *http.ServeMux, logger log.Logger, sched *Scheduler) {
	mux.HandleFunc(LeasePath, func(w http.ResponseWriter, r *http.Request) {
		var req LeaseRequest
		if !decode(w, r, &req) {
			return
		}
		task, err := sched.Lease(r.Context(), req)
		if err != nil {
			level.Warn(logger).Log("msg", "could not lease a task to a worker", "worker", req.WorkerID, "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		encode(w, LeaseResponse{Task: task})
	})

	mux.HandleFunc(HeartbeatPath, func(w http.ResponseWriter, r *http.Request) {
		var req HeartbeatRequest
		if !decode(w, r, &req) {
			return
		}
		encode(w, sched.Heartbeat(req))
	})

	mux.HandleFunc(ResultPath, func(w http.ResponseWriter, r *http.Request) {
		var res Result
		if !decode(w, r, &res) {
			return
		}
		if err := sched.Report(r.Context(), res); err != nil {
			level.Warn(logger).Log("msg", "could not record a task result", "task", res.TaskID, "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}

func decode(w http.ResponseWriter, r *http.Request, into any) bool {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(into); err != nil {
		http.Error(w, errors.Wrap(err, "decode request").Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func encode(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// HTTPClient talks to a manager over HTTP.
//
// The manager address may be a plain host:port or a Thanos service discovery
// address such as dnssrv+_http._tcp.thanos-compact-manager.thanos.svc, which is
// resolved periodically. There is exactly one manager per shard, so whichever
// address resolves first is used.
type HTTPClient struct {
	logger   log.Logger
	client   *http.Client
	provider *dns.Provider
	address  string

	mtx      sync.Mutex
	resolved []string
	lastSync time.Time
}

// NewHTTPClient returns a client for the manager reachable at address.
func NewHTTPClient(logger log.Logger, provider *dns.Provider, address string, timeout time.Duration) *HTTPClient {
	if timeout <= 0 {
		timeout = time.Minute
	}
	return &HTTPClient{
		logger:   logger,
		client:   &http.Client{Timeout: timeout},
		provider: provider,
		address:  address,
	}
}

// endpoint resolves the manager address, refreshing service discovery at most
// once every 30 seconds.
func (c *HTTPClient) endpoint(ctx context.Context) (string, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if !dns.IsDynamicNode(c.address) {
		return c.address, nil
	}
	if len(c.resolved) > 0 && time.Since(c.lastSync) < 30*time.Second {
		return c.resolved[0], nil
	}

	if err := c.provider.Resolve(ctx, []string{c.address}, true); err != nil {
		if len(c.resolved) > 0 {
			// Keep using what we had rather than stalling on a DNS blip.
			level.Warn(c.logger).Log("msg", "could not resolve the manager address; using the previous result", "err", err)
			return c.resolved[0], nil
		}
		return "", errors.Wrap(err, "resolve manager address")
	}

	addrs := c.provider.Addresses()
	if len(addrs) == 0 {
		return "", errors.Errorf("manager address %s resolved to nothing", c.address)
	}
	c.resolved = addrs
	c.lastSync = time.Now()
	return addrs[0], nil
}

func (c *HTTPClient) do(ctx context.Context, path string, req, resp any) error {
	addr, err := c.endpoint(ctx)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}

	body, err := json.Marshal(req)
	if err != nil {
		return errors.Wrap(err, "marshal request")
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+path, bytes.NewReader(body))
	if err != nil {
		return errors.Wrap(err, "build request")
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.client.Do(httpReq)
	if err != nil {
		return errors.Wrapf(err, "call %s", path)
	}
	defer runutil.ExhaustCloseWithLogOnErr(c.logger, httpResp.Body, "close response body")

	if httpResp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(httpResp.Body, 4<<10))
		return errors.Errorf("%s returned %s: %s", path, httpResp.Status, strings.TrimSpace(string(msg)))
	}
	if resp == nil {
		return nil
	}
	if err := json.NewDecoder(httpResp.Body).Decode(resp); err != nil {
		return errors.Wrapf(err, "decode response from %s", path)
	}
	return nil
}

// Lease implements TaskClient.
func (c *HTTPClient) Lease(ctx context.Context, req LeaseRequest) (*Task, error) {
	var resp LeaseResponse
	if err := c.do(ctx, LeasePath, req, &resp); err != nil {
		return nil, err
	}
	return resp.Task, nil
}

// Heartbeat implements TaskClient.
func (c *HTTPClient) Heartbeat(ctx context.Context, req HeartbeatRequest) (HeartbeatResponse, error) {
	var resp HeartbeatResponse
	if err := c.do(ctx, HeartbeatPath, req, &resp); err != nil {
		return HeartbeatResponse{}, err
	}
	return resp, nil
}

// Report implements TaskClient.
func (c *HTTPClient) Report(ctx context.Context, res Result) error {
	return c.do(ctx, ResultPath, res, nil)
}

var _ TaskClient = (*HTTPClient)(nil)
var _ fmt.Stringer = Outcome("")

// String makes Outcome printable in logs.
func (o Outcome) String() string { return string(o) }
