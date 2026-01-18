// Package wspr provides WSPR data processing utilities.
// This file contains CSV parsing and file handling utilities.
package wspr

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// =============================================================================
// File Operations (mmap for zero-copy parsing)
// =============================================================================

// MmapFile memory-maps a file for zero-copy reading.
// Returns the mapped data and file handle (must call UnmapFile when done).
func MmapFile(path string) ([]byte, *os.File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	// Direct syscall for maximum control
	data, err := syscall.Mmap(int(f.Fd()), 0, int(info.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("mmap failed: %w", err)
	}

	return data, f, nil
}

// UnmapFile releases mmap resources.
func UnmapFile(data []byte, f *os.File) {
	syscall.Munmap(data)
	if f != nil {
		f.Close()
	}
}

// Legacy compatibility alias
func MmapWsprFile(path string) ([]byte, error) {
	data, f, err := MmapFile(path)
	if err != nil {
		return nil, err
	}
	// Note: This leaks the file handle. Use MmapFile/UnmapFile for proper cleanup.
	f.Close()
	return data, nil
}

// Legacy compatibility alias
func UnmapWsprFile(data []byte) {
	syscall.Munmap(data)
}

// =============================================================================
// CSV Parsing Constants
// =============================================================================

const (
	// Error throttling: don't spam logs with parse errors
	MaxErrorsToLog = 10

	// CSV column indices (WSPRnet archive format)
	ColID           = 0
	ColTimestamp    = 1
	ColReporter     = 2
	ColReporterGrid = 3
	ColSNR          = 4
	ColFrequency    = 5
	ColCallsign     = 6
	ColGrid         = 7
	ColPower        = 8
	ColDrift        = 9
	ColDistance     = 10
	ColAzimuth      = 11
	ColBand         = 12
	ColVersion      = 13
	ColCode         = 14
	ColRxLat        = 15
	ColRxLon        = 16
	ColTxLat        = 17
	ColTxLon        = 18

	// Minimum columns for valid WSPR record
	MinColumns = 15
)

// =============================================================================
// CSV Parsing with Batch Rotation
// =============================================================================

// ParseCsvStreamWithCSVReader parses CSV data from an io.Reader using encoding/csv.
// This is ideal for streaming gzip-compressed files without loading full content into memory.
// Implements batch rotation: calls onBatchFull when batch is full, receives new empty batch.
func ParseCsvStreamWithCSVReader(reader io.Reader, batch *Batch, filePath string, stats *ParseStats, onBatchFull BatchFullCallback) error {
	csvReader := csv.NewReader(reader)
	csvReader.LazyQuotes = true
	csvReader.FieldsPerRecord = -1 // Variable field count (16-19 columns)

	errorCount := 0

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			stats.FailedRows++
			errorCount++
			if errorCount <= MaxErrorsToLog {
				log.Printf("CSV read error (row %d): %v", stats.TotalRowsRead, err)
			}
			continue
		}

		stats.TotalRowsRead++

		// Skip empty rows
		if len(record) == 0 || (len(record) == 1 && record[0] == "") {
			stats.SkippedEmptyRows++
			continue
		}

		// Parse the record into a Spot
		spot, err := ParseCsvRecord(record, stats)
		if err != nil {
			stats.FailedRows++
			errorCount++
			if errorCount <= MaxErrorsToLog {
				log.Printf("Parse error (row %d): %v", stats.TotalRowsRead, err)
			}
			continue
		}

		stats.SuccessfullyParsed++

		// Add to batch
		if !batch.Add(spot) {
			// Batch is full - rotate
			if onBatchFull != nil {
				newBatch, err := onBatchFull(batch)
				if err != nil {
					return err
				}
				if newBatch == nil {
					return nil // Callback requested stop
				}
				batch = newBatch
				batch.Add(spot)
			}
		}
	}

	if errorCount > MaxErrorsToLog {
		log.Printf("... and %d more parse errors (suppressed)", errorCount-MaxErrorsToLog)
	}

	return nil
}

// ParseCsvDataWithCSVReader parses CSV data from a byte slice (typically mmap'd).
// Converts the bytes to a reader and delegates to ParseCsvStreamWithCSVReader.
func ParseCsvDataWithCSVReader(data []byte, batch *Batch, filePath string, stats *ParseStats, onBatchFull BatchFullCallback) error {
	reader := bytes.NewReader(data)
	return ParseCsvStreamWithCSVReader(reader, batch, filePath, stats, onBatchFull)
}

// =============================================================================
// Record Parsing
// =============================================================================

// ParseCsvRecord parses a single CSV record into a Spot struct.
// Handles 16-19 column WSPRnet archive format.
func ParseCsvRecord(record []string, stats *ParseStats) (Spot, error) {
	if len(record) < MinColumns {
		return Spot{}, fmt.Errorf("insufficient columns: got %d, need %d", len(record), MinColumns)
	}

	var spot Spot
	var err error

	// Column 0: ID (UInt64)
	spot.SpotID, err = parseUint64(record[ColID])
	if err != nil {
		return Spot{}, fmt.Errorf("invalid ID: %w", err)
	}

	// Column 1: Timestamp (Unix epoch)
	ts, err := parseInt64(record[ColTimestamp])
	if err != nil {
		return Spot{}, fmt.Errorf("invalid timestamp: %w", err)
	}
	spot.Timestamp = time.Unix(ts, 0).UTC()

	// Column 2: Reporter (callsign, sanitized)
	spot.Reporter = SanitizeCallsignQuiet(strings.TrimSpace(record[ColReporter]))

	// Column 3: Reporter Grid
	spot.ReporterGrid = strings.TrimSpace(record[ColReporterGrid])

	// Column 4: SNR (Int8)
	snr, err := parseInt8(record[ColSNR])
	if err != nil {
		return Spot{}, fmt.Errorf("invalid SNR: %w", err)
	}
	spot.SNR = snr

	// Column 5: Frequency (Hz, stored as UInt64)
	// WSPRnet archives store frequency in MHz with 6 decimal places
	// e.g., "14.097150" = 14097150 Hz
	freqMHz, err := parseFloat64(record[ColFrequency])
	if err != nil {
		return Spot{}, fmt.Errorf("invalid frequency: %w", err)
	}
	spot.Frequency = uint64(freqMHz * 1_000_000)

	// Column 6: Callsign (transmitter, sanitized)
	spot.Callsign = SanitizeCallsignQuiet(strings.TrimSpace(record[ColCallsign]))

	// Column 7: Grid (transmitter)
	spot.Grid = strings.TrimSpace(record[ColGrid])

	// Column 8: Power (dBm)
	power, err := parseInt8(record[ColPower])
	if err != nil {
		return Spot{}, fmt.Errorf("invalid power: %w", err)
	}
	spot.Power = power

	// Column 9: Drift (Hz/min)
	drift, err := parseInt8(record[ColDrift])
	if err != nil {
		return Spot{}, fmt.Errorf("invalid drift: %w", err)
	}
	spot.Drift = drift

	// Column 10: Distance (km)
	dist, err := parseUint32(record[ColDistance])
	if err != nil {
		return Spot{}, fmt.Errorf("invalid distance: %w", err)
	}
	spot.Distance = dist

	// Column 11: Azimuth (degrees)
	az, err := parseUint16(record[ColAzimuth])
	if err != nil {
		return Spot{}, fmt.Errorf("invalid azimuth: %w", err)
	}
	spot.Azimuth = az

	// Column 12: Band - IGNORED, we calculate from frequency
	// Use GetBand for ADIF band normalization
	spot.Band, _ = GetBand(freqMHz)

	// Column 13: Version (software version)
	if len(record) > ColVersion {
		spot.Version = strings.TrimSpace(record[ColVersion])
	}

	// Column 14: Code (decode status)
	if len(record) > ColCode {
		code, _ := parseUint8(record[ColCode])
		spot.Code = code
	}

	// Columns 15-18: Optional coordinates (19-column format)
	if len(record) > ColRxLat {
		spot.RxLat, _ = parseFloat32(record[ColRxLat])
	}
	if len(record) > ColRxLon {
		spot.RxLon, _ = parseFloat32(record[ColRxLon])
	}
	if len(record) > ColTxLat {
		spot.TxLat, _ = parseFloat32(record[ColTxLat])
	}
	if len(record) > ColTxLon {
		spot.TxLon, _ = parseFloat32(record[ColTxLon])
	}

	// Set default mode
	spot.Mode = "WSPR"

	// Track column count for schema validation
	spot.ColumnCount = uint8(len(record))

	return spot, nil
}

// =============================================================================
// Numeric Parsing Helpers
// =============================================================================

func parseUint64(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	return strconv.ParseUint(s, 10, 64)
}

func parseInt64(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

func parseUint32(s string) (uint32, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	v, err := strconv.ParseUint(s, 10, 32)
	return uint32(v), err
}

func parseUint16(s string) (uint16, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	v, err := strconv.ParseUint(s, 10, 16)
	return uint16(v), err
}

func parseUint8(s string) (uint8, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	v, err := strconv.ParseUint(s, 10, 8)
	return uint8(v), err
}

func parseInt8(s string) (int8, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	v, err := strconv.ParseInt(s, 10, 8)
	return int8(v), err
}

func parseFloat64(s string) (float64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	return strconv.ParseFloat(s, 64)
}

func parseFloat32(s string) (float32, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	v, err := strconv.ParseFloat(s, 32)
	return float32(v), err
}
