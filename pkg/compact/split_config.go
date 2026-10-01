// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"fmt"
	"math/bits"
	"slices"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extpromql"
)

// MaxSplitShards is the largest number of shards a stream can be split into.
const MaxSplitShards = 1024

// SplitConfig configures how SplitGrouper splits streams into shards: which streams, into how many shards, and
// which series labels are left out of the hash that assigns series to shards.
type SplitConfig struct {
	// Shards is the number of shards of the streams no override matches. 1 means they are not split.
	Shards int
	// Overrides set the number of shards of the streams they match; the first one that matches applies.
	Overrides []SplitOverride
	// IgnoreLabels are the series labels left out of the shard hash.
	IgnoreLabels []string
}

// SplitOverride sets the number of shards of the streams whose external labels match all of its matchers.
type SplitOverride struct {
	Matchers []*labels.Matcher
	Shards   int
}

func (o SplitOverride) String() string {
	ms := make([]string, 0, len(o.Matchers))
	for _, m := range o.Matchers {
		ms = append(ms, m.String())
	}
	return fmt.Sprintf("{%s}: %d", strings.Join(ms, ", "), o.Shards)
}

// splitOverrideYAML is one entry of the split configuration file.
type splitOverrideYAML struct {
	// Match is a series selector over the stream's external labels, e.g. {tenant_id="big"}.
	Match string `yaml:"match"`
	// Shards is the number of shards of the matching streams.
	Shards int `yaml:"shards"`
}

// ParseSplitOverrides parses the YAML content of the split configuration: a list of entries with a `match` selector
// over the external labels of a stream and the number of `shards` to split the matching streams into. Empty content
// means no overrides.
func ParseSplitOverrides(content []byte) ([]SplitOverride, error) {
	var entries []splitOverrideYAML
	if err := yaml.UnmarshalStrict(content, &entries); err != nil {
		return nil, errors.Wrap(err, "parse split configuration")
	}
	overrides := make([]SplitOverride, 0, len(entries))
	for i, e := range entries {
		if strings.TrimSpace(e.Match) == "" {
			return nil, errors.Errorf("split configuration entry %d: empty match", i)
		}
		matchers, err := extpromql.ParseMetricSelector(e.Match)
		if err != nil {
			return nil, errors.Wrapf(err, "split configuration entry %d: parse match %q", i, e.Match)
		}
		for _, m := range matchers {
			if m.Name == labels.MetricName {
				return nil, errors.Errorf("split configuration entry %d: match %q selects a metric name; it must only select external labels", i, e.Match)
			}
			if m.Name == metadata.CompactorShardIDLabel {
				return nil, errors.Errorf("split configuration entry %d: match %q selects on %s", i, e.Match, metadata.CompactorShardIDLabel)
			}
		}
		if err := validateShardCount(e.Shards); err != nil {
			return nil, errors.Wrapf(err, "split configuration entry %d (%s)", i, e.Match)
		}
		overrides = append(overrides, SplitOverride{Matchers: matchers, Shards: e.Shards})
	}
	return overrides, nil
}

func validateShardCount(n int) error {
	if n < 1 || n > MaxSplitShards || bits.OnesCount(uint(n)) != 1 {
		return errors.Errorf("number of shards must be a power of two between 1 and %d, got %d", MaxSplitShards, n)
	}
	return nil
}

// Validate checks the configuration and normalizes IgnoreLabels (sorted, without repetition).
func (c *SplitConfig) Validate() error {
	if err := validateShardCount(c.Shards); err != nil {
		return errors.Wrap(err, "default")
	}
	for _, o := range c.Overrides {
		if err := validateShardCount(o.Shards); err != nil {
			return errors.Wrapf(err, "override %s", o)
		}
	}
	for _, l := range c.IgnoreLabels {
		if l == "" {
			return errors.New("ignored labels: empty label name")
		}
	}
	c.IgnoreLabels = slices.Compact(slices.Sorted(slices.Values(c.IgnoreLabels)))
	return nil
}

// Enabled tells whether any stream is configured to be split.
func (c SplitConfig) Enabled() bool {
	if c.Shards > 1 {
		return true
	}
	for _, o := range c.Overrides {
		if o.Shards > 1 {
			return true
		}
	}
	return false
}

// ShardsFor returns the configured number of shards of the stream with the given external labels: the shards of the
// first override whose matchers all match, or the default.
func (c SplitConfig) ShardsFor(streamLabels labels.Labels) int {
	for _, o := range c.Overrides {
		if matchesAll(o.Matchers, streamLabels) {
			return o.Shards
		}
	}
	return max(c.Shards, 1)
}

func matchesAll(matchers []*labels.Matcher, lset labels.Labels) bool {
	for _, m := range matchers {
		if !m.Matches(lset.Get(m.Name)) {
			return false
		}
	}
	return true
}

// scheme returns the scheme new splits of a stream use with the given number of shards.
func (c SplitConfig) scheme(shards int) metadata.SplitScheme {
	return metadata.SplitScheme{Hash: metadata.SplitHashStable, Shards: shards, IgnoreLabels: c.IgnoreLabels}
}
