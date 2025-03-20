# OpenTelemetry Histogram Processor

The Histogram Processor is an OpenTelemetry component that converts delta histogram metrics into individual gauge metrics representing specific percentiles, count, and sum values. This makes it easier to monitor and alert on specific percentile thresholds.

## Features

- Converts delta histograms into individual percentile gauge metrics
- Configurable percentiles (default: 50th, 75th, 90th, 95th)
- Optional count and sum metrics
- Configurable metric naming format
- Support for exemplars and attributes
- Accurate percentile calculation using linear interpolation

## Configuration

```yaml
processors:
  histogram:
    # List of percentiles to calculate (default: ["50", "75", "90", "95"])
    percentiles: ["50", "75", "90", "95", "99"]
    
    # Include count metric (default: true)
    include_count: true
    
    # Include sum metric (default: true)
    include_sum: true
    
    # Metric name suffix format (default: "standard")
    # Options:
    #  - "standard": metrics are suffixed with ".p50", ".p75", etc.
    #  - "underscored": metrics are suffixed with "_p50", "_p75", etc.
    metric_name_suffix: "standard"
```

## Example

For a histogram metric named `http_server_request_duration_seconds`, the processor will generate the following metrics:

- `http_server_request_duration_seconds.p50`
- `http_server_request_duration_seconds.p75`
- `http_server_request_duration_seconds.p90`
- `http_server_request_duration_seconds.p95`
- `http_server_request_duration_seconds.count`
- `http_server_request_duration_seconds.sum`

The values represent the respective percentiles of the histogram distribution.

## When to Use

Use this processor when:

1. You need to monitor and alert on specific percentile values
2. Your monitoring system handles gauge metrics better than histograms
3. You want to reduce the cardinality of your metrics

## Implementation Details

- Only delta histograms are processed (cumulative histograms are passed through unchanged)
- Percentile calculations use linear interpolation for accuracy
- Empty histograms (count = 0) are handled gracefully
- Malformed histograms (where bucket counts don't match bucket bounds) are skipped with warnings
- Thread-safe processing with context-aware shutdown

## Integration

### In a Custom Collector

To integrate this processor into your custom OpenTelemetry Collector build:

1. Add this module to your Go module dependencies
2. Register the processor factory in your collector's components registry

```go
import (
	"histogramprocessor"
	"go.opentelemetry.io/collector/component"
)

func registerComponents(factories *component.Factories) error {
	histogramFactory := histogramprocessor.NewFactory()
	factories.Processors[histogramFactory.Type()] = histogramFactory
	return nil
}
```

### Directory Structure

```
histogramprocessor/              # Root directory
│
├── config.go                    # Configuration definition
├── factory.go                   # Processor factory implementation
├── processor.go                 # Main histogram processor logic
├── processor_test.go            # Unit tests
│
├── components/                  # Helper package for registration
│   └── histogram.go             # Component registration
│
├── builder/                     # Builder package for integration
│   └── histogramprocessor.go    # Builder functions
│
├── cmd/                         # Command-line examples
│   └── main.go                  # Example usage
│
└── examples/
    └── config-example.yaml      # Example configuration
```

## Requirements

- Go 1.19 or later
- OpenTelemetry Collector v0.95.0 or later

## Version Information

Current stable version: v0.1.0

This package follows semantic versioning:
- Major version changes indicate breaking API changes
- Minor version changes indicate new features
- Patch version changes indicate bug fixes

Check the [releases page](https://github.com/ArkoseLabs/histogramprocessor/releases) for release notes and version history.

## Testing

Run the tests with:

```bash
go test -v ./...
```

## Continuous Integration

This project uses GitHub Actions for continuous integration:

- **Build and Test**: Runs on every push and pull request to the main branch
    - Builds the project with multiple Go versions
    - Runs tests with race detection
    - Uploads coverage reports to Codecov
    - Runs linting with golangci-lint

- **Release**: Triggered when a tag is pushed
    - Builds binaries for multiple platforms
    - Creates a GitHub release with the built binaries
    - Generates a changelog from commit messages

## License

This project is licensed under the Apache 2.0 License - see the LICENSE file for details.