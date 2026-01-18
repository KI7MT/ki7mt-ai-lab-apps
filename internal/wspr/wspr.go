// Package wspr provides WSPR data processing utilities.
// This package contains parsers, validators, and ClickHouse integration
// for WSPR (Weak Signal Propagation Reporter) data.
//
// Dual-Path Architecture:
//   - GPU Fast Path: Uses CGO to blast raw buffers to RTX 5090
//   - CPU Reference Path: Pure Go implementation for compatibility
//
// Both paths output the same SpotCH (99-byte) struct for ClickHouse.
package wspr

import (
	"time"
)

// =============================================================================
// Schema Constants
// =============================================================================

// SchemaVersion is the current WSPR schema version (synchronized with wspr_structs.h).
const SchemaVersion = 2

// ColumnCount is the number of columns in v2 schema.
const ColumnCount = 17

// SpotSize is the GPU-optimized struct size (128 bytes, 16-byte aligned).
const SpotSize = 128

// SpotCHSize is the ClickHouse RowBinary struct size (99 bytes, no padding).
const SpotCHSize = 99

// =============================================================================
// Spot - Full WSPR Record (matches legacy 21-column schema with coordinates)
// =============================================================================

// Spot represents a single WSPR spot record with full coordinate support.
// This struct is used internally for parsing and processing.
// For ClickHouse I/O, use SpotCH (99 bytes, no padding).
type Spot struct {
	SpotID       uint64    `ch:"id"`            // WSPRnet spot ID
	Timestamp    time.Time `ch:"timestamp"`     // Spot timestamp UTC
	Reporter     string    `ch:"reporter"`      // Receiving station callsign
	ReporterGrid string    `ch:"reporter_grid"` // Receiver Maidenhead grid
	SNR          int8      `ch:"snr"`           // Signal-to-noise ratio dB
	Frequency    uint64    `ch:"frequency"`     // Frequency in Hz (NOT MHz!)
	Callsign     string    `ch:"callsign"`      // Transmitting station callsign
	Grid         string    `ch:"grid"`          // Transmitter Maidenhead grid
	Power        int8      `ch:"power"`         // TX power dBm
	Drift        int8      `ch:"drift"`         // Frequency drift Hz/min
	Distance     uint32    `ch:"distance"`      // Great circle distance km
	Azimuth      uint16    `ch:"azimuth"`       // Bearing degrees 0-359
	Band         int32     `ch:"band"`          // ADIF band ID (from GetBand)
	Mode         string    `ch:"mode"`          // WSPR mode (default "WSPR")
	Version      string    `ch:"version"`       // WSPR software version
	Code         uint8     `ch:"code"`          // Status/decode code
	ColumnCount  uint8     `ch:"column_count"`  // CSV column count for validation

	// Optional coordinate fields (columns 15-18 in 19-column format)
	RxLat float32 `ch:"rx_lat"` // Receiver latitude
	RxLon float32 `ch:"rx_lon"` // Receiver longitude
	TxLat float32 `ch:"tx_lat"` // Transmitter latitude
	TxLon float32 `ch:"tx_lon"` // Transmitter longitude
}

// =============================================================================
// SpotCH - ClickHouse RowBinary Format (99 bytes, no padding)
// =============================================================================

// SpotCH represents a WSPR spot in ClickHouse RowBinary format.
// This struct is 99 bytes with no padding, designed for direct memory
// mapping to/from the CUDA WSPRSpotCH struct.
//
// CRITICAL: Field order and sizes must match wspr_structs.h WSPRSpotCH exactly!
type SpotCH struct {
	ID           uint64   // 8 bytes  - UInt64
	Timestamp    uint32   // 4 bytes  - DateTime (Unix seconds)
	Reporter     [16]byte // 16 bytes - FixedString(16)
	ReporterGrid [8]byte  // 8 bytes  - FixedString(8)
	SNR          int8     // 1 byte   - Int8
	Frequency    uint64   // 8 bytes  - UInt64 (Hz)
	Callsign     [16]byte // 16 bytes - FixedString(16)
	Grid         [8]byte  // 8 bytes  - FixedString(8)
	Power        int8     // 1 byte   - Int8
	Drift        int8     // 1 byte   - Int8
	Distance     uint32   // 4 bytes  - UInt32
	Azimuth      uint16   // 2 bytes  - UInt16
	Band         int32    // 4 bytes  - Int32
	Mode         [8]byte  // 8 bytes  - FixedString(8)
	Version      [8]byte  // 8 bytes  - FixedString(8)
	Code         uint8    // 1 byte   - UInt8
	ColumnCount  uint8    // 1 byte   - UInt8
	// Total: 99 bytes
}

// =============================================================================
// Batch Types
// =============================================================================

// Batch represents a batch of WSPR spots for processing.
// Used for both GPU and CPU processing paths.
type Batch struct {
	Spots    []Spot // Slice of spots (dynamically sized)
	Count    int    // Number of valid spots in batch
	Capacity int    // Maximum capacity
}

// NewBatch creates a new batch with specified capacity.
func NewBatch(capacity int) *Batch {
	return &Batch{
		Spots:    make([]Spot, capacity),
		Count:    0,
		Capacity: capacity,
	}
}

// Reset resets the batch for reuse without reallocating.
func (b *Batch) Reset() {
	b.Count = 0
}

// IsFull returns true if the batch is at capacity.
func (b *Batch) IsFull() bool {
	return b.Count >= b.Capacity
}

// Add adds a spot to the batch. Returns false if batch is full.
func (b *Batch) Add(spot Spot) bool {
	if b.Count >= b.Capacity {
		return false
	}
	b.Spots[b.Count] = spot
	b.Count++
	return true
}

// BatchCH represents a batch of spots in ClickHouse format (99 bytes each).
// This is the output format from both GPU and CPU processors.
type BatchCH struct {
	Spots []SpotCH // Slice of ClickHouse-format spots
	Count int      // Number of valid spots
}

// NewBatchCH creates a new ClickHouse-format batch.
func NewBatchCH(capacity int) *BatchCH {
	return &BatchCH{
		Spots: make([]SpotCH, capacity),
		Count: 0,
	}
}

// =============================================================================
// Processor Interface - Dual Path Architecture
// =============================================================================

// Processor defines the interface for WSPR batch processing.
// Both GPU and CPU implementations must satisfy this interface.
//
// The key insight: Both paths output []SpotCH (99-byte structs).
// The ClickHouse writer doesn't care which processor built it.
type Processor interface {
	// ProcessBatch processes a batch of raw CSV rows and returns ClickHouse-ready spots.
	// Input: batch of parsed Spot structs
	// Output: batch of SpotCH structs ready for ClickHouse RowBinary insert
	ProcessBatch(batch *Batch) (*BatchCH, error)

	// Name returns the processor name for logging ("GPU" or "CPU").
	Name() string

	// IsAvailable returns true if this processor is available on the current system.
	IsAvailable() bool

	// Close releases any resources held by the processor.
	Close() error
}

// ProcessorType identifies the processor implementation.
type ProcessorType int

const (
	ProcessorTypeAuto ProcessorType = iota // Auto-detect best available
	ProcessorTypeGPU                       // Force GPU (CGO + CUDA)
	ProcessorTypeCPU                       // Force CPU (pure Go)
)

// =============================================================================
// Parse Statistics
// =============================================================================

// ParseStats holds statistics for a parsing operation.
type ParseStats struct {
	TotalRowsRead      int64 // Total rows read from CSV
	SuccessfullyParsed int64 // Rows successfully parsed
	FailedRows         int64 // Rows that failed to parse
	SkippedEmptyRows   int64 // Empty rows skipped
	SanitizedCallsigns int64 // Callsigns that were sanitized
}

// =============================================================================
// Batch Callback for Streaming Parse
// =============================================================================

// BatchFullCallback is called when a batch is full during parsing.
// It should process/send the full batch and return a new empty batch.
// Return nil to stop parsing.
type BatchFullCallback func(fullBatch *Batch) (*Batch, error)

// =============================================================================
// Conversion Utilities
// =============================================================================

// SpotToSpotCH converts a Spot to SpotCH (strips padding for ClickHouse).
// This is the "padding strip" operation for the CPU path.
func SpotToSpotCH(spot *Spot) SpotCH {
	ch := SpotCH{
		ID:          spot.SpotID,
		Timestamp:   uint32(spot.Timestamp.Unix()),
		SNR:         spot.SNR,
		Frequency:   spot.Frequency,
		Power:       spot.Power,
		Drift:       spot.Drift,
		Distance:    spot.Distance,
		Azimuth:     spot.Azimuth,
		Band:        spot.Band,
		Code:        spot.Code,
		ColumnCount: spot.ColumnCount,
	}

	// Copy fixed-size strings (null-padded)
	copy(ch.Reporter[:], spot.Reporter)
	copy(ch.ReporterGrid[:], spot.ReporterGrid)
	copy(ch.Callsign[:], spot.Callsign)
	copy(ch.Grid[:], spot.Grid)
	copy(ch.Mode[:], spot.Mode)
	copy(ch.Version[:], spot.Version)

	return ch
}

// BatchToBatchCH converts a full Batch to BatchCH for ClickHouse.
func BatchToBatchCH(batch *Batch) *BatchCH {
	ch := NewBatchCH(batch.Count)
	for i := 0; i < batch.Count; i++ {
		ch.Spots[i] = SpotToSpotCH(&batch.Spots[i])
	}
	ch.Count = batch.Count
	return ch
}

// =============================================================================
// Legacy Compatibility Aliases
// =============================================================================

// WsprBatch is an alias for Batch (legacy compatibility).
type WsprBatch = Batch

// NewWsprBatch creates a new WsprBatch (legacy compatibility).
func NewWsprBatch(capacity int) *WsprBatch {
	return NewBatch(capacity)
}
