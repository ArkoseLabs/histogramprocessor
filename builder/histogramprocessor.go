package builder

import (
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/processor"

	// Use the local path for the components package
	"histogramprocessor/components"
)

// Components creates and returns the set of processors
func Components() (map[component.Type]processor.Factory, error) {
	// Create a processors map
	processors := make(map[component.Type]processor.Factory)

	// Register the histogram processor
	err := components.RegisterHistogramProcessor(processors)
	if err != nil {
		return nil, fmt.Errorf("failed to register histogram processor: %w", err)
	}

	return processors, nil
}
