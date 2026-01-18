package common

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Stats holds atomic counters for telemetry tracking
type Stats struct {
	TotalRowsProcessed  uint64 // Atomic counter for total rows processed
	TotalBytesRead      uint64 // Atomic counter for total bytes read
	CurrentBatchLatency uint64 // Atomic counter for batch latency in nanoseconds

	// Internal state for reporter
	running   atomic.Bool
	stopCh    chan struct{}
	silent    bool
	lastRows  uint64
	lastBytes uint64
	lastTime  time.Time

	// Moving average window for Mrps calculation
	mrpsWindow     []float64
	mrpsWindowSize int
	mrpsIndex      int
}

// NewStats creates a new Stats instance
func NewStats() *Stats {
	return &Stats{
		stopCh:         make(chan struct{}),
		mrpsWindow:     make([]float64, 10), // 10-sample moving average (5 seconds)
		mrpsWindowSize: 10,
		mrpsIndex:      0,
	}
}

// AddRows atomically increments the total rows processed counter
func (s *Stats) AddRows(count uint64) {
	atomic.AddUint64(&s.TotalRowsProcessed, count)
}

// AddBytes atomically increments the total bytes read counter
func (s *Stats) AddBytes(count uint64) {
	atomic.AddUint64(&s.TotalBytesRead, count)
}

// SetBatchLatency atomically sets the current batch latency in nanoseconds
func (s *Stats) SetBatchLatency(ns uint64) {
	atomic.StoreUint64(&s.CurrentBatchLatency, ns)
}

// GetTotalRows atomically reads the total rows processed
func (s *Stats) GetTotalRows() uint64 {
	return atomic.LoadUint64(&s.TotalRowsProcessed)
}

// GetTotalBytes atomically reads the total bytes read
func (s *Stats) GetTotalBytes() uint64 {
	return atomic.LoadUint64(&s.TotalBytesRead)
}

// GetBatchLatency atomically reads the current batch latency
func (s *Stats) GetBatchLatency() uint64 {
	return atomic.LoadUint64(&s.CurrentBatchLatency)
}

// SetSilent enables or disables silent mode
func (s *Stats) SetSilent(silent bool) {
	s.silent = silent
}

// StartReporter starts a background goroutine that prints telemetry stats
// every 500ms using standard newline-based logging to avoid conflicts with log.Printf
func (s *Stats) StartReporter() {
	if s.running.Load() {
		return // Already running
	}

	s.running.Store(true)
	s.lastTime = time.Now()
	s.lastRows = 0
	s.lastBytes = 0

	go s.reporterLoop()
}

// StopReporter stops the background reporter goroutine
func (s *Stats) StopReporter() {
	if !s.running.Load() {
		return
	}

	s.running.Store(false)
	close(s.stopCh)

	// No need for extra newline since we're using standard logging format
}

// reporterLoop is the background goroutine that periodically prints stats
func (s *Stats) reporterLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.printStatus()
		}
	}
}

// printStatus prints the current telemetry status using standard logging
// Uses newline-based output to avoid conflicts with log.Printf statements
func (s *Stats) printStatus() {
	if s.silent {
		return
	}

	now := time.Now()
	elapsed := now.Sub(s.lastTime).Seconds()

	if elapsed < 0.001 {
		// Avoid division by zero on first tick
		return
	}

	// Get current counters
	currentRows := s.GetTotalRows()
	currentBytes := s.GetTotalBytes()
	batchLatencyNs := s.GetBatchLatency()

	// Calculate deltas
	deltaRows := currentRows - s.lastRows
	deltaBytes := currentBytes - s.lastBytes

	// Calculate throughput
	mibPerSec := (float64(deltaBytes) / (1024 * 1024)) / elapsed
	mrps := (float64(deltaRows) / 1_000_000) / elapsed

	// Update moving average for Mrps
	s.mrpsWindow[s.mrpsIndex] = mrps
	s.mrpsIndex = (s.mrpsIndex + 1) % s.mrpsWindowSize

	// Calculate smoothed Mrps (moving average)
	var sum float64
	var count int
	for i := 0; i < s.mrpsWindowSize; i++ {
		if s.mrpsWindow[i] > 0 {
			sum += s.mrpsWindow[i]
			count++
		}
	}
	smoothedMrps := 0.0
	if count > 0 {
		smoothedMrps = sum / float64(count)
	}

	// Format batch latency
	batchLatencyMs := float64(batchLatencyNs) / 1_000_000

	// Standard logging format (newline-based) to avoid conflicts with log.Printf
	// [CPU]: Changed from [GPU] to reflect CPU-only pipeline
	// [Parse]: Throughput in millions of rows per second
	fmt.Printf("[Progress] Throughput: %.2f MiB/s | Parse: %.2f Mrps (avg: %.2f) | Batch: %.2f ms | Total: %d rows\n",
		mibPerSec,
		mrps,
		smoothedMrps,
		batchLatencyMs,
		currentRows,
	)

	// Update last values
	s.lastRows = currentRows
	s.lastBytes = currentBytes
	s.lastTime = now
}

// Reset resets all counters (useful for testing or restarting)
func (s *Stats) Reset() {
	atomic.StoreUint64(&s.TotalRowsProcessed, 0)
	atomic.StoreUint64(&s.TotalBytesRead, 0)
	atomic.StoreUint64(&s.CurrentBatchLatency, 0)
	s.lastRows = 0
	s.lastBytes = 0
	s.lastTime = time.Now()

	// Clear moving average window
	for i := range s.mrpsWindow {
		s.mrpsWindow[i] = 0
	}
	s.mrpsIndex = 0
}
