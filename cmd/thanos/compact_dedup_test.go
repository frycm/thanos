// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"testing"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/compact"
)

func TestDedupFuncFor(t *testing.T) {
	for _, tc := range []struct {
		name    string
		conf    compactConfig
		wantErr bool
	}{
		{name: "default merger", conf: compactConfig{}},
		{name: "penalty with a replica label", conf: compactConfig{dedupFunc: compact.DedupAlgorithmPenalty, dedupReplicaLabels: []string{"replica"}}},
		{name: "penalty without a replica label", conf: compactConfig{dedupFunc: compact.DedupAlgorithmPenalty}, wantErr: true},
		{name: "penalty with an empty replica label", conf: compactConfig{dedupFunc: compact.DedupAlgorithmPenalty, dedupReplicaLabels: []string{""}}, wantErr: true},
		{name: "unknown func", conf: compactConfig{dedupFunc: "other"}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, err := dedupFuncFor(tc.conf)
			if tc.wantErr {
				testutil.NotOk(t, err)
				return
			}
			testutil.Ok(t, err)
			testutil.Assert(t, f != nil, "a merge function")
		})
	}
}
