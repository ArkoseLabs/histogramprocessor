package histogramprocessor

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"math"
	"testing"
	"time"
)

type test struct {
	name       string
	metricList []string
	inMetrics  pmetric.Metrics
	outMetrics pmetric.Metrics
}

type histogramPoint struct {
	count  uint64
	sum    float64
	bounds []float64
	counts []uint64
}

type histogramMetric struct {
	name   string
	points []histogramPoint
}

type gaugeMetric struct {
	name   string
	points []float64
}

var cases = []test{
	{
		name: "base test",
		inMetrics: histogramToMetrics([]histogramMetric{
			{
				name: "metric_1",
				points: []histogramPoint{
					{
						count:  20,
						sum:    200,
						bounds: []float64{1, 5, 10, 20, 50, 75, 100},
						counts: []uint64{0, 1, 3, 7, 12, 19, 20, 20},
					},
					{
						count:  5,
						sum:    100,
						bounds: []float64{1, 5, 10, 20, 50, 75, 100},
						counts: []uint64{1, 0, 0, 0, 0, 0, 4, 0},
					},
					{
						count:  8,
						sum:    321,
						bounds: []float64{1, 5, 10, 20, 50, 75, 100},
						counts: []uint64{1, 2, 3, 4, 5, 6, 7, 8},
					},
				},
			},
		}),
		outMetrics: gaugeToMetrics([]gaugeMetric{
			{
				name:   "metric_1.p50",
				points: []float64{40, 85, 20},
			},
			{
				name:   "metric_1.p75",
				points: []float64{62.5, 90, 75},
			},
			{
				name:   "metric_1.p90",
				points: []float64{71.875, 95, 100},
			},
			{
				name:   "metric_1.p95",
				points: []float64{75, 95, 100},
			},
			{
				name:   "metric_1.count",
				points: []float64{20, 5, 8},
			},
			{
				name:   "metric_1.sum",
				points: []float64{200, 100, 321},
			},
		}),
	},
}

func gaugeToMetrics(metrics []gaugeMetric) pmetric.Metrics {
	md := pmetric.NewMetrics()
	now := time.Now()

	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	ms := sm.Metrics()
	for _, metric := range metrics {
		m := ms.AppendEmpty()
		m.SetName(metric.name)
		m.SetEmptyGauge()
		gm := m.Gauge()
		for _, value := range metric.points {
			newM := gm.DataPoints().AppendEmpty()
			newM.SetTimestamp(pcommon.NewTimestampFromTime(now))
			newM.SetDoubleValue(value)
		}
	}
	return md
}

func histogramToMetrics(metrics []histogramMetric) pmetric.Metrics {
	md := pmetric.NewMetrics()
	now := time.Now()

	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	ms := sm.Metrics()
	for _, metric := range metrics {
		m := ms.AppendEmpty()
		m.SetName(metric.name)
		m.SetEmptyHistogram()
		hm := m.Histogram().DataPoints()
		m.Histogram().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		for _, value := range metric.points {
			hdp := hm.AppendEmpty()
			hdp.SetCount(value.count)
			hdp.SetSum(value.sum)
			hdp.BucketCounts().FromRaw(value.counts)
			hdp.ExplicitBounds().FromRaw(value.bounds)
			hdp.SetTimestamp(pcommon.NewTimestampFromTime(now))
		}
	}
	return md
}

func TestCases(t *testing.T) {
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			// next stores the results of the filter metric processor
			next := new(consumertest.MetricsSink)
			cfg := &Config{
				//Metrics:           []string{},
			}
			factory := NewFactory()
			mgp, err := factory.CreateMetricsProcessor(
				context.Background(),
				componenttest.NewNopProcessorCreateSettings(),
				cfg,
				next,
			)
			assert.NotNil(t, mgp)
			assert.Nil(t, err)

			caps := mgp.Capabilities()
			assert.True(t, caps.MutatesData)
			ctx := context.Background()
			require.NoError(t, mgp.Start(ctx, nil))

			cErr := mgp.ConsumeMetrics(context.Background(), testCase.inMetrics)
			assert.Nil(t, cErr)
			got := next.AllMetrics()

			require.Equal(t, 1, len(got))
			require.Equal(t, testCase.outMetrics.ResourceMetrics().Len(), got[0].ResourceMetrics().Len())

			expectedMetrics := testCase.outMetrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
			actualMetrics := got[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()

			require.Equal(t, expectedMetrics.Len(), actualMetrics.Len())

			for i := 0; i < expectedMetrics.Len(); i++ {
				eM := expectedMetrics.At(i)
				aM := actualMetrics.At(i)

				require.Equal(t, eM.Name(), aM.Name())

				if eM.Type() == pmetric.MetricTypeGauge {
					eDataPoints := eM.Gauge().DataPoints()
					aDataPoints := aM.Gauge().DataPoints()
					require.Equal(t, eDataPoints.Len(), aDataPoints.Len())

					for j := 0; j < eDataPoints.Len(); j++ {
						require.Equal(t, eDataPoints.At(j).DoubleValue(), aDataPoints.At(j).DoubleValue(), fmt.Sprintf("Result metric (%s) value differ", eM.Name()))
					}
				}

				if eM.Type() == pmetric.MetricTypeHistogram {
					eDataPoints := eM.Histogram().DataPoints()
					aDataPoints := aM.Histogram().DataPoints()

					require.Equal(t, eDataPoints.Len(), aDataPoints.Len())
					require.Equal(t, eM.Histogram().AggregationTemporality(), aM.Histogram().AggregationTemporality())

					for j := 0; j < eDataPoints.Len(); j++ {
						require.Equal(t, eDataPoints.At(j).Count(), aDataPoints.At(j).Count())
						require.Equal(t, eDataPoints.At(j).HasSum(), aDataPoints.At(j).HasSum())
						if math.IsNaN(eDataPoints.At(j).Sum()) {
							require.True(t, math.IsNaN(aDataPoints.At(j).Sum()))
						} else {
							require.Equal(t, eDataPoints.At(j).Sum(), aDataPoints.At(j).Sum())
						}
						require.Equal(t, eDataPoints.At(j).BucketCounts().AsRaw(), aDataPoints.At(j).BucketCounts().AsRaw())
					}
				}
			}

			require.NoError(t, mgp.Shutdown(ctx))
		})
	}
}
