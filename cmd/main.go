package main

import (
	"log"

	"histogramprocessor/builder"
)

func main() {
	// Get all processor components
	processors, err := builder.Components()
	if err != nil {
		log.Fatalf("Failed to initialize components: %v", err)
	}

	// Log the registered processors
	log.Printf("Registered %d processors", len(processors))

	for processorType := range processors {
		log.Printf("  - %s", processorType.String())
	}
}
