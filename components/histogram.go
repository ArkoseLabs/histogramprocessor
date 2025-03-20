package components

import (
	"histogramprocessor" // Local import without GitHub URL

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/processor"
)

// RegisterHistogramProcessor registers the histogram processor with the provided factories map
func RegisterHistogramProcessor(processors map[component.Type]processor.Factory) error {
	histogramFactory := histogramprocessor.NewFactory()
	processors[histogramFactory.Type()] = histogramFactory
	return nil
}
