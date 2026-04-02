use crate::ModifierStore;
use ::redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::collections::HashMap;
use std::path::Path;
use std::sync::RwLock;

const PRIMARY: TableDefinition<(u8, [u8; 32]), &[u8]> = TableDefinition::new("primary");
const HEIGHT_INDEX: TableDefinition<(u8, u32), [u8; 32]> = TableDefinition::new("height_index");

/// redb-backed modifier store.
pub struct RedbModifierStore {
    db: Database,
    tips: RwLock<HashMap<u8, (u32, [u8; 32])>>,
}

/// Error type wrapping redb's various error kinds.
#[derive(Debug)]
pub enum StoreError {
    Database(::redb::DatabaseError),
    Transaction(::redb::TransactionError),
    Table(::redb::TableError),
    Storage(::redb::StorageError),
    Commit(::redb::CommitError),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(e) => write!(f, "database: {e}"),
            Self::Transaction(e) => write!(f, "transaction: {e}"),
            Self::Table(e) => write!(f, "table: {e}"),
            Self::Storage(e) => write!(f, "storage: {e}"),
            Self::Commit(e) => write!(f, "commit: {e}"),
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Database(e) => Some(e),
            Self::Transaction(e) => Some(e),
            Self::Table(e) => Some(e),
            Self::Storage(e) => Some(e),
            Self::Commit(e) => Some(e),
        }
    }
}

impl From<::redb::DatabaseError> for StoreError {
    fn from(e: ::redb::DatabaseError) -> Self {
        Self::Database(e)
    }
}

impl From<::redb::TransactionError> for StoreError {
    fn from(e: ::redb::TransactionError) -> Self {
        Self::Transaction(e)
    }
}

impl From<::redb::TableError> for StoreError {
    fn from(e: ::redb::TableError) -> Self {
        Self::Table(e)
    }
}

impl From<::redb::StorageError> for StoreError {
    fn from(e: ::redb::StorageError) -> Self {
        Self::Storage(e)
    }
}

impl From<::redb::CommitError> for StoreError {
    fn from(e: ::redb::CommitError) -> Self {
        Self::Commit(e)
    }
}

impl RedbModifierStore {
    /// Opens or creates a redb database at the given path.
    pub fn new(path: &Path) -> Result<Self, StoreError> {
        let db = Database::create(path)?;
        let tips = Self::load_tips(&db)?;
        Ok(Self {
            db,
            tips: RwLock::new(tips),
        })
    }

    /// Scans the height index to reconstruct tip state per modifier type.
    /// Keys are sorted `(type_id, height)`, so the last entry per type_id wins.
    fn load_tips(db: &Database) -> Result<HashMap<u8, (u32, [u8; 32])>, StoreError> {
        let read_txn = db.begin_read()?;
        let table = match read_txn.open_table(HEIGHT_INDEX) {
            Ok(t) => t,
            Err(::redb::TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
            Err(e) => return Err(StoreError::Table(e)),
        };

        let mut tips = HashMap::new();
        for result in table.iter()? {
            let (key_guard, value_guard) = result?;
            let (type_id, height) = key_guard.value();
            let id = value_guard.value();
            tips.insert(type_id, (height, id));
        }
        Ok(tips)
    }
}

impl ModifierStore for RedbModifierStore {
    type Error = StoreError;

    fn put(
        &self,
        type_id: u8,
        id: &[u8; 32],
        height: u32,
        data: &[u8],
    ) -> Result<(), Self::Error> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(PRIMARY)?;
            let _ = table.insert((type_id, *id), data)?;
        }
        {
            let mut table = write_txn.open_table(HEIGHT_INDEX)?;
            let _ = table.insert((type_id, height), *id)?;
        }
        write_txn.commit()?;

        let mut tips = self.tips.write().unwrap_or_else(|e| e.into_inner());
        if tips.get(&type_id).is_none_or(|tip| height > tip.0) {
            tips.insert(type_id, (height, *id));
        }
        Ok(())
    }

    fn put_batch(
        &self,
        entries: &[(u8, [u8; 32], u32, Vec<u8>)],
    ) -> Result<(), Self::Error> {
        let write_txn = self.db.begin_write()?;
        {
            let mut primary = write_txn.open_table(PRIMARY)?;
            let mut height_idx = write_txn.open_table(HEIGHT_INDEX)?;
            for (type_id, id, height, data) in entries {
                let _ = primary.insert((*type_id, *id), data.as_slice())?;
                let _ = height_idx.insert((*type_id, *height), *id)?;
            }
        }
        write_txn.commit()?;

        let mut tips = self.tips.write().unwrap_or_else(|e| e.into_inner());
        for (type_id, id, height, _) in entries {
            if tips.get(type_id).is_none_or(|tip| *height > tip.0) {
                tips.insert(*type_id, (*height, *id));
            }
        }
        Ok(())
    }

    fn get(
        &self,
        type_id: u8,
        id: &[u8; 32],
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        let read_txn = self.db.begin_read()?;
        let table = match read_txn.open_table(PRIMARY) {
            Ok(t) => t,
            Err(::redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(StoreError::Table(e)),
        };
        let value = table.get((type_id, *id))?;
        Ok(value.map(|guard| guard.value().to_vec()))
    }

    fn get_id_at(
        &self,
        type_id: u8,
        height: u32,
    ) -> Result<Option<[u8; 32]>, Self::Error> {
        let read_txn = self.db.begin_read()?;
        let table = match read_txn.open_table(HEIGHT_INDEX) {
            Ok(t) => t,
            Err(::redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(StoreError::Table(e)),
        };
        let value = table.get((type_id, height))?;
        Ok(value.map(|guard| guard.value()))
    }

    fn contains(
        &self,
        type_id: u8,
        id: &[u8; 32],
    ) -> Result<bool, Self::Error> {
        let read_txn = self.db.begin_read()?;
        let table = match read_txn.open_table(PRIMARY) {
            Ok(t) => t,
            Err(::redb::TableError::TableDoesNotExist(_)) => return Ok(false),
            Err(e) => return Err(StoreError::Table(e)),
        };
        Ok(table.get((type_id, *id))?.is_some())
    }

    fn tip(
        &self,
        type_id: u8,
    ) -> Result<Option<(u32, [u8; 32])>, Self::Error> {
        let tips = self.tips.read().unwrap_or_else(|e| e.into_inner());
        Ok(tips.get(&type_id).copied())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_store() -> (RedbModifierStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = RedbModifierStore::new(&dir.path().join("test.redb")).unwrap();
        (store, dir)
    }

    fn test_id(byte: u8) -> [u8; 32] {
        let mut id = [0u8; 32];
        id[0] = byte;
        id
    }

    #[test]
    fn round_trip() {
        let (store, _dir) = test_store();
        let id = test_id(1);
        let data = b"hello world";

        store.put(101, &id, 1, data).unwrap();
        let result = store.get(101, &id).unwrap();
        assert_eq!(result, Some(data.to_vec()));
    }

    #[test]
    fn batch_atomicity() {
        let (store, _dir) = test_store();
        let entries = vec![
            (101, test_id(1), 1, b"data1".to_vec()),
            (101, test_id(2), 2, b"data2".to_vec()),
            (102, test_id(3), 1, b"data3".to_vec()),
        ];

        store.put_batch(&entries).unwrap();

        assert_eq!(store.get(101, &test_id(1)).unwrap(), Some(b"data1".to_vec()));
        assert_eq!(store.get(101, &test_id(2)).unwrap(), Some(b"data2".to_vec()));
        assert_eq!(store.get(102, &test_id(3)).unwrap(), Some(b"data3".to_vec()));
    }

    #[test]
    fn height_index() {
        let (store, _dir) = test_store();
        let id = test_id(1);

        store.put(101, &id, 42, b"block data").unwrap();
        let result = store.get_id_at(101, 42).unwrap();
        assert_eq!(result, Some(id));
    }

    #[test]
    fn tip_tracking() {
        let (store, _dir) = test_store();

        store.put(101, &test_id(1), 10, b"a").unwrap();
        assert_eq!(store.tip(101).unwrap(), Some((10, test_id(1))));

        store.put(101, &test_id(2), 20, b"b").unwrap();
        assert_eq!(store.tip(101).unwrap(), Some((20, test_id(2))));

        // Lower height should not update tip
        store.put(101, &test_id(3), 5, b"c").unwrap();
        assert_eq!(store.tip(101).unwrap(), Some((20, test_id(2))));
    }

    #[test]
    fn contains_present_and_absent() {
        let (store, _dir) = test_store();
        let id = test_id(1);

        assert!(!store.contains(101, &id).unwrap());
        store.put(101, &id, 1, b"data").unwrap();
        assert!(store.contains(101, &id).unwrap());
    }

    #[test]
    fn idempotent_put() {
        let (store, _dir) = test_store();
        let id = test_id(1);
        let data = b"same data";

        store.put(101, &id, 1, data).unwrap();
        store.put(101, &id, 1, data).unwrap();
        assert_eq!(store.get(101, &id).unwrap(), Some(data.to_vec()));
    }

    #[test]
    fn empty_store() {
        let (store, _dir) = test_store();

        assert_eq!(store.tip(101).unwrap(), None);
        assert_eq!(store.get(101, &test_id(1)).unwrap(), None);
        assert_eq!(store.get_id_at(101, 0).unwrap(), None);
        assert!(!store.contains(101, &test_id(1)).unwrap());
    }
}
