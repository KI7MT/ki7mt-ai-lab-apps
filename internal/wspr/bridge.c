// bridge.c - WSPR struct size bridge functions for CGO
// These provide the handshake between Go and the CUDA library
//
// Build: Compiled automatically by CGO when building with -tags cuda

#include <stdint.h>
#include "wspr_structs.h"

// Return the GPU struct size (128 bytes, 16-byte aligned)
uint32_t wspr_get_struct_size(void) {
    return WSPR_SPOT_SIZE_BYTES;
}

// Return the ClickHouse struct size (99 bytes, no padding)
uint32_t wspr_get_ch_struct_size(void) {
    return WSPR_CLICKHOUSE_ROW_SIZE;
}

// Return the schema version for compatibility checking
uint32_t wspr_get_schema_version(void) {
    return WSPR_SCHEMA_VERSION;
}
