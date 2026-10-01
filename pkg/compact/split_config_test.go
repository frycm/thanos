// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"strings"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"
)

func TestParseSplitOverrides(t *testing.T) {
	t.Parallel()

	overrides, err := ParseSplitOverrides([]byte(`
- match: '{tenant_id="big"}'
  shards: 16
- match: '{tenant_id=~"big|large", cluster!="dev"}'
  shards: 4
- match: '{tenant_id="quiet"}'
  shards: 1
`))
	testutil.Ok(t, err)
	testutil.Equals(t, 3, len(overrides))
	testutil.Equals(t, `{tenant_id="big"}: 16`, overrides[0].String())
	testutil.Equals(t, `{tenant_id=~"big|large", cluster!="dev"}: 4`, overrides[1].String())

	overrides, err = ParseSplitOverrides([]byte(""))
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(overrides))

	for _, tc := range []struct {
		name, content, err string
	}{
		{name: "not a power of two", content: "- match: '{a=\"1\"}'\n  shards: 3", err: "power of two"},
		{name: "zero shards", content: "- match: '{a=\"1\"}'", err: "power of two"},
		{name: "too many shards", content: "- match: '{a=\"1\"}'\n  shards: 2048", err: "power of two between 1 and 1024"},
		{name: "empty match", content: "- shards: 2", err: "empty match"},
		{name: "invalid selector", content: "- match: '{a=}'\n  shards: 2", err: "parse match"},
		{name: "metric name", content: "- match: 'up{a=\"1\"}'\n  shards: 2", err: "metric name"},
		{name: "shard label", content: "- match: '{__compactor_shard_id__=\"1_of_2\"}'\n  shards: 2", err: "__compactor_shard_id__"},
		{name: "unknown field", content: "- match: '{a=\"1\"}'\n  shards: 2\n  shard: 2", err: "field shard not found"},
		{name: "not a list", content: "match: '{a=\"1\"}'", err: "parse split configuration"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseSplitOverrides([]byte(tc.content))
			testutil.NotOk(t, err)
			testutil.Assert(t, strings.Contains(err.Error(), tc.err), "unexpected error: %v", err)
		})
	}
}

func TestSplitConfig(t *testing.T) {
	t.Parallel()

	overrides, err := ParseSplitOverrides([]byte(`
- match: '{tenant_id="big"}'
  shards: 16
- match: '{tenant_id=~"big|large"}'
  shards: 4
- match: '{tenant_id="quiet"}'
  shards: 1
`))
	testutil.Ok(t, err)
	cfg := SplitConfig{Shards: 2, Overrides: overrides, IgnoreLabels: []string{"replica", "a", "replica"}}
	testutil.Ok(t, cfg.Validate())
	testutil.Equals(t, []string{"a", "replica"}, cfg.IgnoreLabels)
	testutil.Assert(t, cfg.Enabled())

	// The first matching override wins; others use the default.
	testutil.Equals(t, 16, cfg.ShardsFor(labels.FromStrings("tenant_id", "big", "cluster", "eu")))
	testutil.Equals(t, 4, cfg.ShardsFor(labels.FromStrings("tenant_id", "large")))
	testutil.Equals(t, 1, cfg.ShardsFor(labels.FromStrings("tenant_id", "quiet")))
	testutil.Equals(t, 2, cfg.ShardsFor(labels.FromStrings("tenant_id", "other")))
	testutil.Equals(t, 2, cfg.ShardsFor(labels.EmptyLabels()))

	// Disabled by default; an override enables it.
	testutil.Assert(t, !(SplitConfig{Shards: 1}).Enabled())
	testutil.Assert(t, !(SplitConfig{Shards: 1, Overrides: overrides[2:]}).Enabled())
	testutil.Assert(t, (SplitConfig{Shards: 1, Overrides: overrides[1:]}).Enabled())

	for _, bad := range []SplitConfig{
		{Shards: 0},
		{Shards: 6},
		{Shards: 2, IgnoreLabels: []string{""}},
		{Shards: 1, Overrides: []SplitOverride{{Shards: 5}}},
	} {
		testutil.NotOk(t, bad.Validate())
	}
}
