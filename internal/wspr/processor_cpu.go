// Package wspr provides WSPR data processing utilities.
package wspr

import (
	"runtime"
	"sync"
)

// =============================================================================
// CPU Processor - Pure Go Reference Implementation
// =============================================================================

// CPUProcessor implements the Processor interface using pure Go.
// This is the reference implementation that works on any platform.
// Optimized for parallel processing on multi-core CPUs (AMD Ryzen 9 9950X3D).
type CPUProcessor struct {
	numWorkers int        // Number of parallel workers
	pool       *sync.Pool // Pool for reusable SpotCH slices
}

// NewCPUProcessor creates a new CPU-based processor.
// numWorkers specifies the number of parallel goroutines (0 = auto-detect).
func NewCPUProcessor(numWorkers int) *CPUProcessor {
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	return &CPUProcessor{
		numWorkers: numWorkers,
		pool: &sync.Pool{
			New: func() interface{} {
				// Pre-allocate slice for batch processing
				return make([]SpotCH, 0, 10000)
			},
		},
	}
}

// Name returns the processor name.
func (p *CPUProcessor) Name() string {
	return "CPU"
}

// IsAvailable returns true (CPU is always available).
func (p *CPUProcessor) IsAvailable() bool {
	return true
}

// Close releases resources (no-op for CPU processor).
func (p *CPUProcessor) Close() error {
	return nil
}

// ProcessBatch processes a batch of spots using parallel Go routines.
// This is the CPU reference implementation that matches GPU output exactly.
//
// Processing steps (per spot):
//   1. Sanitize callsign and reporter fields
//   2. Normalize band from frequency
//   3. Convert to SpotCH (99-byte ClickHouse format)
//
// Thread-safety: Safe to call from multiple goroutines.
func (p *CPUProcessor) ProcessBatch(batch *Batch) (*BatchCH, error) {
	if batch == nil || batch.Count == 0 {
		return NewBatchCH(0), nil
	}

	// For small batches, process sequentially (avoid goroutine overhead)
	if batch.Count < 1000 || p.numWorkers <= 1 {
		return p.processSequential(batch), nil
	}

	// For large batches, process in parallel
	return p.processParallel(batch), nil
}

// processSequential processes a batch without parallelization.
// Used for small batches where goroutine overhead isn't worth it.
func (p *CPUProcessor) processSequential(batch *Batch) *BatchCH {
	result := NewBatchCH(batch.Count)

	for i := 0; i < batch.Count; i++ {
		spot := &batch.Spots[i]

		// Step 1: Sanitize callsign and reporter
		spot.Callsign = SanitizeCallsignQuiet(spot.Callsign)
		spot.Reporter = SanitizeCallsignQuiet(spot.Reporter)

		// Step 2: Normalize band from frequency (if not already set)
		if spot.Band == 0 && spot.Frequency > 0 {
			freqMHz := float64(spot.Frequency) / 1_000_000.0
			spot.Band, _ = GetBand(freqMHz)
		}

		// Step 3: Set default mode if empty
		if spot.Mode == "" {
			spot.Mode = "WSPR"
		}

		// Step 4: Convert to ClickHouse format
		result.Spots[i] = SpotToSpotCH(spot)
	}

	result.Count = batch.Count
	return result
}

// processParallel processes a batch using multiple goroutines.
// Splits the batch into chunks and processes them concurrently.
func (p *CPUProcessor) processParallel(batch *Batch) *BatchCH {
	result := NewBatchCH(batch.Count)

	// Calculate chunk size
	chunkSize := (batch.Count + p.numWorkers - 1) / p.numWorkers

	var wg sync.WaitGroup

	for workerID := 0; workerID < p.numWorkers; workerID++ {
		start := workerID * chunkSize
		end := start + chunkSize
		if end > batch.Count {
			end = batch.Count
		}
		if start >= batch.Count {
			break
		}

		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			p.processChunk(batch, result, start, end)
		}(start, end)
	}

	wg.Wait()
	result.Count = batch.Count
	return result
}

// processChunk processes a range of spots within a batch.
// Called by parallel workers.
func (p *CPUProcessor) processChunk(batch *Batch, result *BatchCH, start, end int) {
	for i := start; i < end; i++ {
		spot := &batch.Spots[i]

		// Step 1: Sanitize callsign and reporter
		spot.Callsign = SanitizeCallsignQuiet(spot.Callsign)
		spot.Reporter = SanitizeCallsignQuiet(spot.Reporter)

		// Step 2: Normalize band from frequency (if not already set)
		if spot.Band == 0 && spot.Frequency > 0 {
			freqMHz := float64(spot.Frequency) / 1_000_000.0
			spot.Band, _ = GetBand(freqMHz)
		}

		// Step 3: Set default mode if empty
		if spot.Mode == "" {
			spot.Mode = "WSPR"
		}

		// Step 4: Convert to ClickHouse format
		result.Spots[i] = SpotToSpotCH(spot)
	}
}

// =============================================================================
// CPU Processor Factory
// =============================================================================

// Ensure CPUProcessor implements Processor interface
var _ Processor = (*CPUProcessor)(nil)
