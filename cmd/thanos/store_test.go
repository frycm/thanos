// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

// TestStoreFlags_BlockResolutionDefaults pins the defaults to the resolution
// levels the compactor produces: with them the resolution filter is a no-op,
// so a store started without the flags serves every block as before. The
// levels are milliseconds, and a millisecond count read as a time.Duration is
// nanoseconds, which once turned the 1h maximum into 3.6ms and hid every
// downsampled block by default.
func TestStoreFlags_BlockResolutionDefaults(t *testing.T) {
	app := kingpin.New("store", "")
	conf := &storeConfig{}
	conf.registerFlag(app)
	_, err := app.Parse(nil)
	testutil.Ok(t, err)

	testutil.Equals(t, time.Duration(0), time.Duration(conf.minBlockResolution))
	testutil.Equals(t, time.Hour, time.Duration(conf.maxBlockResolution))
	testutil.Equals(t, downsample.ResLevel2, time.Duration(conf.maxBlockResolution).Milliseconds())
}

func TestStoreFlags_BlockResolutionValidation(t *testing.T) {
	testutil.Ok(t, validateBlockResolutions(0, time.Hour))
	testutil.Ok(t, validateBlockResolutions(5*time.Minute, 5*time.Minute))
	testutil.NotOk(t, validateBlockResolutions(time.Minute, time.Hour), "a minimum that is not a downsampling level hides nothing and must be refused")
	testutil.NotOk(t, validateBlockResolutions(0, 2*time.Hour), "a maximum that is not a downsampling level must be refused")
	testutil.NotOk(t, validateBlockResolutions(time.Hour, 5*time.Minute), "min above max must be refused")

	err := validateBlockResolutions(time.Minute, time.Hour)
	testutil.Assert(t, strings.Contains(err.Error(), "use one of 0s, 5m, 1h"), "the error must name the levels as durations: %v", err)
}
