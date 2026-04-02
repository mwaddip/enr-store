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
}
