package histogramprocessor

import (
	"context"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"math"
	"strconv"
)

type histogramProcessor struct {
	logger     *zap.Logger
	cancelFunc context.CancelFunc
}

func newHistogramProcessor(config *Config, logger *zap.Logger) *histogramProcessor {
	_, cancel := context.WithCancel(context.Background())
	p := &histogramProcessor{
		logger:     logger,
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
	percentiles := []string{"50", "75", "90", "95"}
	newMs.EnsureCapacity(len(percentiles) + 2)
	for _, p := range percentiles {
		initMetric(in, metric, newMs, "p"+p)
	}
	// count and sum
	initMetric(in, metric, newMs, "count")
	initMetric(in, metric, newMs, "sum")

	for i := 0; i < in.DataPoints().Len(); i++ {
		hDP := in.DataPoints().At(i)
		bounds := hDP.ExplicitBounds()
		counts := hDP.BucketCounts()
		totalCount := hDP.Count()
		putDP(newMs.At(len(percentiles)), hDP, float64(hDP.Count()))
		putDP(newMs.At(len(percentiles)+1), hDP, hDP.Sum())
		if totalCount == 0 {
			// don't send precalculated histogram metrics, as they are useless
			continue
		}

		if counts.Len() > 0 && counts.Len() != bounds.Len()+1 {
			continue
		}

		if counts.At(counts.Len()-1) != totalCount {
			preprocessBuckets(hDP.BucketCounts())
		}

		for i, p := range percentiles {
			newM := newMs.At(i)
			pf, err := strconv.ParseUint(p, 10, 64)
			if err != nil {
				continue
			}
			putDP(newM, hDP, getPercentileFromBuckets(pf, totalCount, bounds, counts))
		}
	}
	newMs.RemoveIf(func(metric pmetric.Metric) bool {
		return metric.Gauge().DataPoints().Len() == 0
	})
	return newMs
}

func preprocessBuckets(counts pcommon.UInt64Slice) {
	for i := 0; i < counts.Len(); i++ {
		if i > 0 {
			counts.SetAt(i, counts.At(i-1)+counts.At(i))
		}
	}
}

func putDP(newM pmetric.Metric, hDP pmetric.HistogramDataPoint, value float64) {
	gDP := newM.Gauge().DataPoints().AppendEmpty()
	hDP.Attributes().CopyTo(gDP.Attributes())
	hDP.Exemplars().CopyTo(gDP.Exemplars())
	gDP.SetFlags(hDP.Flags())
	gDP.SetStartTimestamp(hDP.StartTimestamp())
	gDP.SetTimestamp(hDP.Timestamp())
	gDP.SetDoubleValue(value)
}

func initMetric(in pmetric.Histogram, metric pmetric.Metric, newMs pmetric.MetricSlice, p string) {
	newM := newMs.AppendEmpty()
	newM.SetName(metric.Name() + "." + p)
	newM.SetDescription(metric.Description())
	newM.SetUnit(metric.Unit())
	newM.SetEmptyGauge().DataPoints().EnsureCapacity(in.DataPoints().Len())
}

func getPercentileFromBuckets(pf uint64, totalCount uint64, bounds pcommon.Float64Slice, counts pcommon.UInt64Slice) float64 {
	pf = totalCount * pf / 100
	left := 0
	right := counts.Len() - 1
	for (left != right) && (left+1 != right) {
		middle := (left + right) / 2
		if counts.At(middle) >= pf {
			right = middle
		} else {
			left = middle
		}
	}
	if right == counts.Len()-1 { // last value, meaning out of bounds.
		return math.Inf(1)
	}
	if left == right { // should only happen if we have uniform distribution of points, i.e. no measurements in buckets
		return bounds.At(right)
	}
	// calculate point value depending on measurement counts in the bucket

	lbound := bounds.At(left)
	rbound := bounds.At(right)
	bucketCounts := counts.At(right) - counts.At(left)
	countOffset := pf - counts.At(left)
	return ((rbound-lbound)/float64(bucketCounts+1))*float64(1+countOffset) + lbound
}

func (hp *histogramProcessor) shutdown(context.Context) error {
	hp.cancelFunc()
	return nil
}
