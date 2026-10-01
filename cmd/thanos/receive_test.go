// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"
)

func TestValidateSeriesReplicaLabelName(t *testing.T) {
	lset := labels.FromStrings("receive_replica", "0")
	for _, tc := range []struct {
		name, splitTenant string
		expectErr         bool
	}{
		{name: ""},
		{name: "prometheus_replica"},
		{name: "prometheus_replica", splitTenant: "tenant"},
		{name: "prometheus.replica", expectErr: true},
		{name: "1replica", expectErr: true},
		{name: "tenant_id", expectErr: true},
		{name: "tenant", splitTenant: "tenant", expectErr: true},
		{name: "receive_replica", expectErr: true},
	} {
		err := validateSeriesReplicaLabelName(&receiveConfig{
			seriesReplicaLabelName: tc.name,
			tenantLabelName:        "tenant_id",
			splitTenantLabelName:   tc.splitTenant,
		}, lset)
		if tc.expectErr {
			testutil.NotOk(t, err, tc.name)
			continue
		}
		testutil.Ok(t, err, tc.name)
	}
}
