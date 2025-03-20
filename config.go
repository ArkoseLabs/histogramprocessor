package histogramprocessor

// Config holds the configuration for the histogram processor
type Config struct {
	// Percentiles is a list of percentiles to calculate from the histogram
	// If not specified, defaults to [50, 75, 90, 95]
	Percentiles []string `mapstructure:"percentiles"`

	// IncludeCount determines if the count metric should be included
	// Default: true
	IncludeCount *bool `mapstructure:"include_count"`

	// IncludeSum determines if the sum metric should be included
	// Default: true
	IncludeSum *bool `mapstructure:"include_sum"`

	// MetricNameSuffix configures the suffix format for derived metrics
	// It can be:
	// - "standard": metrics are suffixed with ".p50", ".p75", etc.
	// - "underscored": metrics are suffixed with "_p50", "_p75", etc.
	// Default: "standard"
	MetricNameSuffix string `mapstructure:"metric_name_suffix"`
}

func (c *Config) setDefaults() {
	if len(c.Percentiles) == 0 {
		c.Percentiles = []string{"50", "75", "90", "95"}
	}

	if c.IncludeCount == nil {
		trueValue := true
		c.IncludeCount = &trueValue
	}

	if c.IncludeSum == nil {
		trueValue := true
		c.IncludeSum = &trueValue
	}

	if c.MetricNameSuffix == "" {
		c.MetricNameSuffix = "standard"
	}
}

// GetSeparator returns the separator to use between the metric name and the suffix
func (c *Config) GetSeparator() string {
	if c.MetricNameSuffix == "underscored" {
		return "_"
	}
	return "."
}
