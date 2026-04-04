# enr-store

Persistent modifier storage for [ergo-node-rust](https://github.com/mwaddip/ergo-node-rust). Stores block headers, block sections, and other modifiers as pre-validated, pre-serialized bytes in a [redb](https://github.com/cberner/redb) embedded database.

## What it does

- **Primary storage** keyed by `(type_id, modifier_id)` for raw modifier bytes
- **Height index** for non-header modifiers (`type_id, height` lookups)
- **Fork-aware header storage** supporting multiple headers per block height, cumulative difficulty scores, and atomic best-chain switching for chain reorganization
- **Migration** from single-header-per-height schema to fork-aware schema on first open

## Trait: `ModifierStore`

The public API is the `ModifierStore` trait in `src/lib.rs`. The redb implementation (`RedbModifierStore`) is the only backend. The trait exists so upstream components depend on the contract, not the storage engine.

Key method groups:

| Group | Methods |
|-------|---------|
| Generic modifiers | `put`, `put_batch`, `get`, `get_id_at`, `contains`, `tip` |
| Fork-aware headers | `put_header`, `put_header_batch`, `header_ids_at_height`, `header_score`, `best_header_at`, `best_header_tip`, `switch_best_chain` |

## Tables

| Table | Key | Value | Purpose |
|-------|-----|-------|---------|
| `primary` | `(type_id, modifier_id)` | raw bytes | All modifier data |
| `height_index` | `(type_id, height)` | modifier_id | Height lookup for non-header types |
| `header_forks` | `(height, fork)` | header_id | Multiple headers per height |
| `header_scores` | `header_id` | BigUint bytes | Cumulative difficulty per header |
| `best_chain` | `height` | header_id | Current best chain (one per height) |

## Building and testing

See the [main ergo-node-rust repo](https://github.com/mwaddip/ergo-node-rust) for build instructions and project documentation.

```
cargo test
```
