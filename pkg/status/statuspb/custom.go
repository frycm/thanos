// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package statuspb

import (
	"cmp"
	"slices"

	v1 "github.com/prometheus/prometheus/web/api/v1"
)

func NewTSDBStatisticsResponse(statistics *TSDBStatistics) *TSDBStatisticsResponse {
	return &TSDBStatisticsResponse{
		Result: &TSDBStatisticsResponse_Statistics{
			Statistics: statistics,
		},
	}
}

func NewWarningTSDBStatisticsResponse(warning error) *TSDBStatisticsResponse {
	return &TSDBStatisticsResponse{
		Result: &TSDBStatisticsResponse_Warning{
			Warning: warning.Error(),
		},
	}
}

// Merge merges the provided TSDBStatisticsEntry with the receiver.
func (tse *TSDBStatisticsEntry) Merge(stats *TSDBStatisticsEntry) {
	tse.HeadStatistics.NumSeries += stats.HeadStatistics.NumSeries
	tse.HeadStatistics.NumLabelPairs += stats.HeadStatistics.NumLabelPairs
	tse.HeadStatistics.ChunkCount += stats.HeadStatistics.ChunkCount

	if tse.HeadStatistics.MinTime <= 0 || tse.HeadStatistics.MinTime > stats.HeadStatistics.MinTime {
		tse.HeadStatistics.MinTime = stats.HeadStatistics.MinTime
	}

	if tse.HeadStatistics.MaxTime < stats.HeadStatistics.MaxTime {
		tse.HeadStatistics.MaxTime = stats.HeadStatistics.MaxTime
	}

	tse.SeriesCountByMetricName = mergeStatistics(tse.SeriesCountByMetricName, stats.SeriesCountByMetricName, addValue)
	// The same label values may exist on different instances so it makes more
	// sense to keep the max value rather than adding them all.
	tse.LabelValueCountByLabelName = mergeStatistics(tse.LabelValueCountByLabelName, stats.LabelValueCountByLabelName, maxValue)
	tse.MemoryInBytesByLabelName = mergeStatistics(tse.MemoryInBytesByLabelName, stats.MemoryInBytesByLabelName, addValue)
	tse.SeriesCountByLabelValuePair = mergeStatistics(tse.SeriesCountByLabelValuePair, stats.SeriesCountByLabelValuePair, addValue)
}

func addValue(a, b uint64) uint64 {
	return a + b
}

func maxValue(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func mergeStatistics(a, b []Statistic, mergeFunc func(uint64, uint64) uint64) []Statistic {
	// Keep the order in which names were first seen, so that statistics with
	// equal values sort deterministically instead of in map iteration order.
	merged := make(map[string]int, len(a))
	out := make([]Statistic, 0, len(a)+len(b))
	for _, stat := range a {
		merged[stat.Name] = len(out)
		out = append(out, stat)
	}

	for _, stat := range b {
		i, found := merged[stat.Name]
		if !found {
			merged[stat.Name] = len(out)
			out = append(out, stat)
			continue
		}
		out[i].Value = mergeFunc(out[i].Value, stat.Value)
	}

	if len(out) == 0 {
		return nil
	}
	slices.SortStableFunc(out, func(a, b Statistic) int {
		// Descending sort.
		return cmp.Compare(b.Value, a.Value)
	})
	return out
}

// ConvertToPrometheusTSDBStat converts a protobuf Statistic slice to the equivalent Prometheus struct.
func ConvertToPrometheusTSDBStat(stats []Statistic) []v1.TSDBStat {
	ret := make([]v1.TSDBStat, len(stats))
	for i := range stats {
		ret[i] = v1.TSDBStat{
			Name:  stats[i].Name,
			Value: stats[i].Value,
		}
	}

	return ret
}
