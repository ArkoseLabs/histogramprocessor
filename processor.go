package histogramprocessor

import (
	"context"
	"math"
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type histogramProcessor struct {
	logger     *zap.Logger
	config     *Config
	cancelFunc context.CancelFunc
}

func newHistogramProcessor(config *Config, logger *zap.Logger) *histogramProcessor {
	_, cancel := context.WithCancel(context.Background())
	p := &histogramProcessor{
		logger:     logger,
		config:     config,
		cancelFunc: cancel,
	}
	return p
}

func (hp *histogramProcessor) processMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	rms := md.ResourceMetrics()
	rms.RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		sms := rm.ScopeMetrics()
		sms.RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			ms := sm.Metrics()
			newMs := pmetric.NewMetricSlice()
			ms.RemoveIf(func(metric pmetric.Metric) bool {
				switch metric.Type() {
				case pmetric.MetricTypeHistogram:
					mh := metric.Histogram()
					if mh.AggregationTemporality() != pmetric.AggregationTemporalityDelta {
						return false
					}
					hp.convertMetrics(mh, metric).MoveAndAppendTo(newMs)
					return true
				default:
					return false
				}
			})
			newMs.MoveAndAppendTo(ms)
			return sm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})
	return md, nil
}

func (hp *histogramProcessor) convertMetrics(in pmetric.Histogram, metric pmetric.Metric) pmetric.MetricSlice {
	newMs := pmetric.NewMetricSlice()
	percentiles := hp.config.Percentiles

	// Calculate capacity: percentiles + potential count and sum
	capacity := len(percentiles)
	if *hp.config.IncludeCount {
		capacity++
	}
	if *hp.config.IncludeSum {
		capacity++
	}

	newMs.EnsureCapacity(capacity)
	separator := hp.config.GetSeparator()

	// Create metrics for percentiles
	for _, p := range percentiles {
		initMetric(in, metric, newMs, separator+"p"+p)
	}

	// Add count and sum if configured
	countIdx := -1
	sumIdx := -1

	if *hp.config.IncludeCount {
		countIdx = newMs.Len()
		initMetric(in, metric, newMs, separator+"count")
	}

	if *hp.config.IncludeSum {
		sumIdx = newMs.Len()
		initMetric(in, metric, newMs, separator+"sum")
	}

	for i := 0; i < in.DataPoints().Len(); i++ {
		hDP := in.DataPoints().At(i)
		bounds := hDP.ExplicitBounds()
		counts := hDP.BucketCounts()
		totalCount := hDP.Count()

		// Add count and sum metrics if configured
		if countIdx >= 0 {
			putDP(newMs.At(countIdx), hDP, float64(totalCount))
		}

		if sumIdx >= 0 {
			putDP(newMs.At(sumIdx), hDP, hDP.Sum())
		}

		if totalCount == 0 {
			// skip percentile calculations for empty histograms
			continue
		}

		if counts.Len() > 0 && counts.Len() != bounds.Len()+1 {
			hp.logger.Warn("Bucket counts length doesn't match bounds length + 1",
				zap.Int("counts_len", counts.Len()),
				zap.Int("bounds_len", bounds.Len()),
				zap.String("metric_name", metric.Name()))
			continue
		}

		// Create a copy of counts for processing to avoid modifying the original data
		cumulativeCounts := pcommon.NewUInt64Slice()
		cumulativeCounts.EnsureCapacity(counts.Len())
		for j := 0; j < counts.Len(); j++ {
			cumulativeCounts.Append(counts.At(j))
		}

		// Ensure cumulative bucket counts
		if cumulativeCounts.At(cumulativeCounts.Len()-1) != totalCount {
			preprocessBuckets(cumulativeCounts)
		}

		for j, p := range percentiles {
			pf, err := strconv.ParseUint(p, 10, 64)
			if err != nil {
				hp.logger.Warn("Failed to parse percentile", zap.String("percentile", p), zap.Error(err))
				continue
			}
			percentileValue := getPercentileFromBuckets(pf, totalCount, bounds, cumulativeCounts)
			putDP(newMs.At(j), hDP, percentileValue)
		}
	}

	// Remove any metrics with no data points
	newMs.RemoveIf(func(metric pmetric.Metric) bool {
		return metric.Gauge().DataPoints().Len() == 0
	})

	return newMs
}

// preprocessBuckets converts non-cumulative bucket counts to cumulative
func preprocessBuckets(counts pcommon.UInt64Slice) {
	for i := 1; i < counts.Len(); i++ {
		counts.SetAt(i, counts.At(i-1)+counts.At(i))
	}
}

// putDP adds a new data point to the gauge metric with values from the histogram data point
func putDP(newM pmetric.Metric, hDP pmetric.HistogramDataPoint, value float64) {
	gDP := newM.Gauge().DataPoints().AppendEmpty()
	hDP.Attributes().CopyTo(gDP.Attributes())
	hDP.Exemplars().CopyTo(gDP.Exemplars())
	gDP.SetFlags(hDP.Flags())
	gDP.SetStartTimestamp(hDP.StartTimestamp())
	gDP.SetTimestamp(hDP.Timestamp())
	gDP.SetDoubleValue(value)
}

// initMetric creates a new gauge metric with the specified suffix
func initMetric(in pmetric.Histogram, metric pmetric.Metric, newMs pmetric.MetricSlice, suffix string) {
	newM := newMs.AppendEmpty()
	newM.SetName(metric.Name() + suffix)
	newM.SetDescription(metric.Description())
	newM.SetUnit(metric.Unit())
	newM.SetEmptyGauge().DataPoints().EnsureCapacity(in.DataPoints().Len())
}

// getPercentileFromBuckets calculates the percentile value from histogram buckets
func getPercentileFromBuckets(pf uint64, totalCount uint64, bounds pcommon.Float64Slice, counts pcommon.UInt64Slice) float64 {
	// Calculate the count threshold for the percentile
	threshold := totalCount * pf / 100

	// Edge case: if threshold is 0, return the lowest bound or -infinity
	if threshold == 0 {
		if bounds.Len() > 0 {
			return bounds.At(0)
		}
		return math.Inf(-1)
	}

	// Binary search to find the bucket containing the percentile
	left := 0
	right := counts.Len() - 1

	for (left != right) && (left+1 != right) {
		middle := (left + right) / 2
		if counts.At(middle) >= threshold {
			right = middle
		} else {
			left = middle
		}
	}

	// Handle edge cases
	if right == counts.Len()-1 && counts.At(left) < threshold {
		// Value is in the overflow bucket
		return math.Inf(1)
	}

	if counts.At(left) >= threshold {
		// Value is in or below the first bucket
		if left == 0 {
			// Below the first bound - use the lower bound
			if bounds.Len() > 0 {
				return bounds.At(0)
			}
			return math.Inf(-1)
		}
		return bounds.At(left)
	}

	if left == right {
		// Uniform distribution within the bucket
		return bounds.At(right)
	}

	// Linear interpolation within the bucket
	lbound := bounds.At(left)
	rbound := bounds.At(right)
	bucketCounts := counts.At(right) - counts.At(left)
	countOffset := threshold - counts.At(left)

	// Calculate the position within the bucket and interpolate
	return ((rbound-lbound)/float64(bucketCounts+1))*float64(1+countOffset) + lbound
}

// shutdown is called when the processor is being shut down
func (hp *histogramProcessor) shutdown(context.Context) error {
	hp.cancelFunc()
	return nil
}
