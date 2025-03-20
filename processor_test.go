package histogramprocessor

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

func TestHistogramProcessor_ProcessMetrics(t *testing.T) {
	// Create configuration with default values
	cfg := &Config{}
	cfg.setDefaults()

	// Create processor
	processor := newHistogramProcessor(cfg, zap.NewNop())

	// Create test metrics
	metrics := createTestMetrics()
	processedMetrics, err := processor.processMetrics(context.Background(), metrics)
	require.NoError(t, err)

	// Get the output metrics and validate
	rms := processedMetrics.ResourceMetrics()
	assert.Equal(t, 1, rms.Len())

	sms := rms.At(0).ScopeMetrics()
	assert.Equal(t, 1, sms.Len())

	ms := sms.At(0).Metrics()

	// Original metrics + derived metrics (4 percentiles + count + sum)
	// The original histogram metric is removed and 6 metrics are added
	assert.Equal(t, 7, ms.Len())

	// Check for the derived metrics
	foundP50 := false
	foundP75 := false
	foundP90 := false
	foundP95 := false
	foundCount := false
	foundSum := false

	for i := 0; i < ms.Len(); i++ {
		metric := ms.At(i)

		switch metric.Name() {
		case "test_histogram.p50":
			foundP50 = true
			assert.Equal(t, pmetric.MetricTypeGauge, metric.Type())
			assert.Equal(t, 1, metric.Gauge().DataPoints().Len())
			dp := metric.Gauge().DataPoints().At(0)
			assert.InDelta(t, 45.0, dp.DoubleValue(), 0.1) // Around 50th percentile
		case "test_histogram.p75":
			foundP75 = true
			assert.Equal(t, pmetric.MetricTypeGauge, metric.Type())
			assert.Equal(t, 1, metric.Gauge().DataPoints().Len())
			dp := metric.Gauge().DataPoints().At(0)
			assert.InDelta(t, 80.0, dp.DoubleValue(), 0.1) // Around 75th percentile
		case "test_histogram.p90":
			foundP90 = true
			assert.Equal(t, pmetric.MetricTypeGauge, metric.Type())
			assert.Equal(t, 1, metric.Gauge().DataPoints().Len())
			dp := metric.Gauge().DataPoints().At(0)
			assert.InDelta(t, 95.0, dp.DoubleValue(), 0.1) // Around 90th percentile
		case "test_histogram.p95":
			foundP95 = true
			assert.Equal(t, pmetric.MetricTypeGauge, metric.Type())
			assert.Equal(t, 1, metric.Gauge().DataPoints().Len())
			dp := metric.Gauge().DataPoints().At(0)
			assert.InDelta(t, 98.0, dp.DoubleValue(), 0.1) // Around 95th percentile
		case "test_histogram.count":
			foundCount = true
			assert.Equal(t, pmetric.MetricTypeGauge, metric.Type())
			assert.Equal(t, 1, metric.Gauge().DataPoints().Len())
			dp := metric.Gauge().DataPoints().At(0)
			assert.Equal(t, 100.0, dp.DoubleValue())
		case "test_histogram.sum":
			foundSum = true
			assert.Equal(t, pmetric.MetricTypeGauge, metric.Type())
			assert.Equal(t, 1, metric.Gauge().DataPoints().Len())
			dp := metric.Gauge().DataPoints().At(0)
			assert.Equal(t, 5000.0, dp.DoubleValue())
		}
	}

	assert.True(t, foundP50, "p50 metric not found")
	assert.True(t, foundP75, "p75 metric not found")
	assert.True(t, foundP90, "p90 metric not found")
	assert.True(t, foundP95, "p95 metric not found")
	assert.True(t, foundCount, "count metric not found")
	assert.True(t, foundSum, "sum metric not found")
}

func TestHistogramProcessor_WithCustomConfig(t *testing.T) {
	// Create configuration with custom values
	trueVal := true
	cfg := &Config{
		Percentiles:      []string{"99"},
		IncludeCount:     &trueVal,
		IncludeSum:       &trueVal,
		MetricNameSuffix: "underscored",
	}

	// Create processor
	processor := newHistogramProcessor(cfg, zap.NewNop())

	// Create test metrics
	metrics := createTestMetrics()
	processedMetrics, err := processor.processMetrics(context.Background(), metrics)
	require.NoError(t, err)

	// Get the output metrics and validate
	rms := processedMetrics.ResourceMetrics()
	sms := rms.At(0).ScopeMetrics()
	ms := sms.At(0).Metrics()

	// Original metrics + derived metrics (1 percentile + count + sum)
	// The original histogram metric is removed and 3 metrics are added
	assert.Equal(t, 4, ms.Len())

	// Check for the derived metrics with underscores
	foundP99 := false
	foundCount := false
	foundSum := false

	for i := 0; i < ms.Len(); i++ {
		metric := ms.At(i)

		switch metric.Name() {
		case "test_histogram_p99":
			foundP99 = true
		case "test_histogram_count":
			foundCount = true
		case "test_histogram_sum":
			foundSum = true
		}
	}

	assert.True(t, foundP99, "p99 metric not found")
	assert.True(t, foundCount, "count metric not found")
	assert.True(t, foundSum, "sum metric not found")
}

func TestHistogramProcessor_EmptyHistogram(t *testing.T) {
	// Create configuration with default values
	cfg := &Config{}
	cfg.setDefaults()

	// Create processor
	processor := newHistogramProcessor(cfg, zap.NewNop())

	// Create metrics with empty histogram
	metrics := createEmptyHistogram()
	processedMetrics, err := processor.processMetrics(context.Background(), metrics)
	require.NoError(t, err)

	// Get the output metrics and validate
	rms := processedMetrics.ResourceMetrics()
	sms := rms.At(0).ScopeMetrics()
	ms := sms.At(0).Metrics()

	// Verify that we still have count and sum metrics, but no percentiles
	// since they can't be calculated for empty histograms
	assert.Equal(t, 3, ms.Len()) // Original + count + sum

	// Check that count and sum are present with zero values
	foundCount := false
	foundSum := false

	for i := 0; i < ms.Len(); i++ {
		metric := ms.At(i)

		if metric.Name() == "test_histogram.count" {
			foundCount = true
			assert.Equal(t, 0.0, metric.Gauge().DataPoints().At(0).DoubleValue())
		} else if metric.Name() == "test_histogram.sum" {
			foundSum = true
			assert.Equal(t, 0.0, metric.Gauge().DataPoints().At(0).DoubleValue())
		}
	}

	assert.True(t, foundCount, "count metric not found")
	assert.True(t, foundSum, "sum metric not found")
}

func createEmptyHistogram() pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	// Add a histogram metric with no data points
	metric := sm.Metrics().AppendEmpty()
	metric.SetName("test_histogram")
	metric.SetDescription("Empty histogram metric")
	metric.SetUnit("ms")

	histogram := metric.SetEmptyHistogram()
	histogram.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

	dp := histogram.DataPoints().AppendEmpty()
	dp.SetCount(0)
	dp.SetSum(0)
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(-time.Minute)))
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	// Add empty buckets
	dp.BucketCounts().FromRaw([]uint64{0, 0, 0, 0, 0})
	dp.ExplicitBounds().FromRaw([]float64{10, 50, 90, 99})

	return md
}

func TestGetPercentileFromBuckets(t *testing.T) {
	// Set up test cases
	tests := []struct {
		name       string
		percentile uint64
		totalCount uint64
		bounds     []float64
		counts     []uint64
		expected   float64
	}{
		{
			name:       "Simple 50th percentile",
			percentile: 50,
			totalCount: 100,
			bounds:     []float64{10, 50, 90, 99},
			counts:     []uint64{10, 30, 60, 90, 100},
			expected:   45.0,
		},
		{
			name:       "Simple 75th percentile",
			percentile: 75,
			totalCount: 100,
			bounds:     []float64{10, 50, 90, 99},
			counts:     []uint64{10, 30, 60, 90, 100},
			expected:   80.0,
		},
		{
			name:       "Zero percentile",
			percentile: 0,
			totalCount: 100,
			bounds:     []float64{10, 50, 90, 99},
			counts:     []uint64{10, 30, 60, 90, 100},
			expected:   10.0, // First bound
		},
		{
			name:       "100th percentile",
			percentile: 100,
			totalCount: 100,
			bounds:     []float64{10, 50, 90, 99},
			counts:     []uint64{10, 30, 60, 90, 100},
			expected:   math.Inf(1), // Infinity (beyond last bound)
		},
		{
			name:       "Percentile in first bucket",
			percentile: 5,
			totalCount: 100,
			bounds:     []float64{10, 50, 90, 99},
			counts:     []uint64{10, 30, 60, 90, 100},
			expected:   10.0, // First bound
		},
		{
			name:       "Empty histogram",
			percentile: 50,
			totalCount: 0,
			bounds:     []float64{10, 50, 90, 99},
			counts:     []uint64{0, 0, 0, 0, 0},
			expected:   10.0, // First bound
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bounds := pcommon.NewFloat64Slice()
			bounds.FromRaw(tt.bounds)

			counts := pcommon.NewUInt64Slice()
			counts.FromRaw(tt.counts)

			result := getPercentileFromBuckets(tt.percentile, tt.totalCount, bounds, counts)

			if math.IsInf(tt.expected, 1) {
				assert.True(t, math.IsInf(result, 1), "Expected positive infinity")
			} else if math.IsInf(tt.expected, -1) {
				assert.True(t, math.IsInf(result, -1), "Expected negative infinity")
			} else {
				assert.InDelta(t, tt.expected, result, 0.1, "Expected %f, got %f", tt.expected, result)
			}
		})
	}
}

func TestPreprocessBuckets(t *testing.T) {
	// Test converting non-cumulative to cumulative buckets
	counts := pcommon.NewUInt64Slice()
	counts.FromRaw([]uint64{5, 10, 15, 20, 50})

	// Expected result after preprocessing
	expected := []uint64{5, 15, 30, 50, 100}

	preprocessBuckets(counts)

	// Verify that the counts are now cumulative
	for i, e := range expected {
		assert.Equal(t, e, counts.At(i), "Bucket %d: expected %d, got %d", i, e, counts.At(i))
	}
}

func createTestMetrics() pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	// Add a histogram metric
	metric := sm.Metrics().AppendEmpty()
	metric.SetName("test_histogram")
	metric.SetDescription("Test histogram metric")
	metric.SetUnit("ms")

	histogram := metric.SetEmptyHistogram()
	histogram.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

	dp := histogram.DataPoints().AppendEmpty()
	dp.SetCount(100)
	dp.SetSum(5000)
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(-time.Minute)))
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	// Add buckets
	dp.BucketCounts().FromRaw([]uint64{10, 20, 30, 30, 10})
	dp.ExplicitBounds().FromRaw([]float64{10, 50, 90, 99})

	// Add a second metric (non-histogram) to ensure it's not touched
	metric2 := sm.Metrics().AppendEmpty()
	metric2.SetName("test_gauge")
	metric2.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(123.45)

	return md
}
