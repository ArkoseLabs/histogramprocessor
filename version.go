package histogramprocessor

// Version contains the current version of the histogram processor.
const Version = "0.1.0"

// BuildInfo contains additional build information like commit hash.
var BuildInfo = struct {
	Commit   string
	Date     string
	BuiltBy  string
	Platform string
}{
	Commit:   "unknown",
	Date:     "unknown",
	BuiltBy:  "unknown",
	Platform: "unknown",
}
