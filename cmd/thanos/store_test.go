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
// so a store gateway started without the flags serves every block as before. The
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
	for _, tcase := range []struct {
		name string

		minResolution time.Duration
		maxResolution time.Duration

		expectedErr string
	}{
		{
			name:          "defaults",
			minResolution: 0,
			maxResolution: time.Hour,
		},
		{
			name:          "min equal to max",
			minResolution: 5 * time.Minute,
			maxResolution: 5 * time.Minute,
		},
		{
			// A minimum that is not a downsampling level hides nothing and must be refused;
			// the error must name the levels as durations.
			name:          "min not a downsampling level",
			minResolution: time.Minute,
			maxResolution: time.Hour,
			expectedErr:   "use one of 0s, 5m, 1h",
		},
		{
			name:          "max not a downsampling level",
			minResolution: 0,
			maxResolution: 2 * time.Hour,
			expectedErr:   "is not a downsampling level",
		},
		{
			name:          "min above max",
			minResolution: time.Hour,
			maxResolution: 5 * time.Minute,
			expectedErr:   "can't be greater than",
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			err := validateBlockResolutions(tcase.minResolution, tcase.maxResolution)
			if tcase.expectedErr == "" {
				testutil.Ok(t, err)
				return
			}
			testutil.NotOk(t, err)
			testutil.Assert(t, strings.Contains(err.Error(), tcase.expectedErr), "expected error containing %q, got: %v", tcase.expectedErr, err)
		})
	}
}
