// Package solar provides solar flux data processing utilities.
// This package handles NOAA solar indices data ingestion and
// correlation analysis with WSPR propagation data.
package solar

// Index represents a solar flux index record.
type Index struct {
	Date       uint32  `ch:"date"`
	SFI        float32 `ch:"sfi"`         // Solar Flux Index (10.7cm)
	SunspotNum int16   `ch:"sunspot_num"` // Sunspot number
	KpIndex    float32 `ch:"kp_index"`    // Planetary K-index
	ApIndex    int16   `ch:"ap_index"`    // Planetary A-index
	Dst        int16   `ch:"dst"`         // Disturbance Storm Time
	F107Adj    float32 `ch:"f107_adj"`    // Adjusted F10.7
}

// SchemaVersion is the current solar schema version.
const SchemaVersion = 1
