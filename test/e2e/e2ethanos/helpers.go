// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2ethanos

import (
	"context"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/e2e"
	"github.com/pkg/errors"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/runutil"
)

func CleanScenario(t testing.TB, e *e2e.DockerEnvironment) func() {
	return func() {
		// Make sure Clean can properly delete everything.
		testutil.Ok(t, exec.Command("chmod", "-R", "777", e.SharedDir()).Run())
		e.Close()
	}
}

// Restart stops the runnable and starts it again once Docker has removed its
// container. The e2e library runs containers with --rm, and docker stop returns
// before Docker removes the stopped container, so starting straight away can
// fail because the container name is still taken.
func Restart(e e2e.Environment, r e2e.Runnable) error {
	if err := r.Stop(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	container := e.Name() + "-" + r.Name()
	if err := runutil.Retry(100*time.Millisecond, ctx.Done(), func() error {
		if exec.Command("docker", "container", "inspect", container).Run() == nil {
			return errors.Errorf("container %s is not removed yet", container)
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "wait for the removal of container %s", container)
	}
	return e2e.StartAndWaitReady(r)
}

func singleJoiningSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	}
	return a + b
}

// NewSingleHostReverseProxy is almost same as httputil.NewSingleHostReverseProxy
// but it performs a url path rewrite.
func NewSingleHostReverseProxy(target *url.URL, externalPrefix string) *httputil.ReverseProxy {
	targetQuery := target.RawQuery
	director := func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.URL.Path = singleJoiningSlash(target.Path, strings.TrimPrefix(req.URL.Path, "/"+externalPrefix))

		if targetQuery == "" || req.URL.RawQuery == "" {
			req.URL.RawQuery = targetQuery + req.URL.RawQuery
		} else {
			req.URL.RawQuery = targetQuery + "&" + req.URL.RawQuery
		}
	}
	return &httputil.ReverseProxy{Director: director}
}
