mod redb;

pub use self::redb::{RedbModifierStore, StoreError};

/// Persistent storage for block-related modifiers.
///
/// A dumb persistence layer: stores pre-validated, pre-serialized bytes
/// keyed by `(type_id, modifier_id, height)`. Does not parse, validate,
/// or interpret modifier content.
pub trait ModifierStore: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Store a single modifier.
    fn put(
        &self,
        type_id: u8,
        id: &[u8; 32],
        height: u32,
        data: &[u8],
    ) -> Result<(), Self::Error>;

    /// Store a batch of modifiers atomically.
    /// All entries are written in a single transaction — all succeed or none do.
    fn put_batch(
        &self,
        entries: &[(u8, [u8; 32], u32, Vec<u8>)],
    ) -> Result<(), Self::Error>;

    /// Retrieve a modifier by type and ID.
    fn get(
        &self,
        type_id: u8,
        id: &[u8; 32],
    ) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Retrieve the modifier ID at a given height for a type.
    fn get_id_at(
        &self,
        type_id: u8,
        height: u32,
    ) -> Result<Option<[u8; 32]>, Self::Error>;

    /// Check whether a modifier exists without reading its data.
    fn contains(
        &self,
        type_id: u8,
        id: &[u8; 32],
    ) -> Result<bool, Self::Error>;

    /// Returns the tip (highest height and its modifier ID) for a type.
    /// None if no modifiers of that type have been stored.
    fn tip(
        &self,
        type_id: u8,
    ) -> Result<Option<(u32, [u8; 32])>, Self::Error>;

    /// Store a header with its fork number and cumulative score.
    /// Writes to PRIMARY (type_id=101), HEADER_FORKS, HEADER_SCORES.
    /// Writes to BEST_CHAIN only if no entry exists at this height yet
    /// (first header at a height is assumed best until a reorg says otherwise).
    fn put_header(
        &self,
        id: &[u8; 32],
        height: u32,
        fork: u32,
        score: &[u8],
        data: &[u8],
    ) -> Result<(), Self::Error>;

    /// Batch version of put_header. All entries written atomically.
    fn put_header_batch(
        &self,
        entries: &[([u8; 32], u32, u32, Vec<u8>, Vec<u8>)],
        // (id, height, fork, score, data)
    ) -> Result<(), Self::Error>;

    /// Get all header IDs at a given height across all forks.
    /// Returns Vec<(header_id, fork_number)> sorted by fork number.
    fn header_ids_at_height(
        &self,
        height: u32,
    ) -> Result<Vec<([u8; 32], u32)>, Self::Error>;

    /// Get the cumulative score for a header.
    fn header_score(
        &self,
        id: &[u8; 32],
    ) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Get the best chain header ID at a height.
    fn best_header_at(
        &self,
        height: u32,
    ) -> Result<Option<[u8; 32]>, Self::Error>;

    /// Get the best chain tip (highest height and header ID).
    fn best_header_tip(&self) -> Result<Option<(u32, [u8; 32])>, Self::Error>;

    /// Atomically switch the best chain: remove old entries, insert new ones.
    /// Updates the best_header_tip cache.
    fn switch_best_chain(
        &self,
        demote: &[u32],
        promote: &[(u32, [u8; 32])],
    ) -> Result<(), Self::Error>;
}
