use crate::ModifierStore;
use ::redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::collections::HashMap;
use std::path::Path;
use std::sync::RwLock;

const PRIMARY: TableDefinition<(u8, [u8; 32]), &[u8]> = TableDefinition::new("primary");
const HEIGHT_INDEX: TableDefinition<(u8, u32), [u8; 32]> = TableDefinition::new("height_index");
const HEADER_FORKS: TableDefinition<(u32, u32), [u8; 32]> = TableDefinition::new("header_forks");
const HEADER_SCORES: TableDefinition<[u8; 32], &[u8]> = TableDefinition::new("header_scores");
const BEST_CHAIN: TableDefinition<u32, [u8; 32]> = TableDefinition::new("best_chain");

/// redb-backed modifier store.
pub struct RedbModifierStore {
    db: Database,
    tips: RwLock<HashMap<u8, (u32, [u8; 32])>>,
    best_header_tip: RwLock<Option<(u32, [u8; 32])>>,
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
        let best_header_tip = Self::load_best_header_tip(&db)?;
        let store = Self {
            db,
            tips: RwLock::new(tips),
            best_header_tip: RwLock::new(best_header_tip),
        };
        store.migrate_headers_if_needed()?;
        Ok(store)
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

    /// Scans BEST_CHAIN to find the highest entry.
    fn load_best_header_tip(db: &Database) -> Result<Option<(u32, [u8; 32])>, StoreError> {
        let read_txn = db.begin_read()?;
        let table = match read_txn.open_table(BEST_CHAIN) {
            Ok(t) => t,
            Err(::redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(StoreError::Table(e)),
        };
        // Keys are u32 sorted ascending; last entry is the tip.
        let result = match table.last()? {
            Some((key_guard, value_guard)) => {
                Some((key_guard.value(), value_guard.value()))
            }
            None => None,
        };
        Ok(result)
    }

    /// Migrates headers from HEIGHT_INDEX (type_id=101) to the new fork-aware tables.
    /// Runs once: skips if HEADER_FORKS already has entries.
    pub fn migrate_headers_if_needed(&self) -> Result<(), StoreError> {
        // Check if already migrated.
        {
            let read_txn = self.db.begin_read()?;
            match read_txn.open_table(HEADER_FORKS) {
                Ok(t) => {
                    if t.len()? > 0 {
                        return Ok(());
                    }
                }
                Err(::redb::TableError::TableDoesNotExist(_)) => {}
                Err(e) => return Err(StoreError::Table(e)),
            }
        }

        // Collect all (101, height) entries from HEIGHT_INDEX.
        let mut entries: Vec<(u32, [u8; 32])> = Vec::new();
        {
            let read_txn = self.db.begin_read()?;
            let table = match read_txn.open_table(HEIGHT_INDEX) {
                Ok(t) => t,
                Err(::redb::TableError::TableDoesNotExist(_)) => return Ok(()),
                Err(e) => return Err(StoreError::Table(e)),
            };
            for result in table.range((101, 0)..=(101, u32::MAX))? {
                let (key_guard, value_guard) = result?;
                let (_type_id, height) = key_guard.value();
                let id = value_guard.value();
                entries.push((height, id));
            }
        }

        if entries.is_empty() {
            return Ok(());
        }

        entries.sort_by_key(|(h, _)| *h);

        let write_txn = self.db.begin_write()?;
        {
            let mut forks = write_txn.open_table(HEADER_FORKS)?;
            let mut scores = write_txn.open_table(HEADER_SCORES)?;
            let mut best = write_txn.open_table(BEST_CHAIN)?;
            let mut height_idx = write_txn.open_table(HEIGHT_INDEX)?;

            for (height, id) in &entries {
                forks.insert((*height, 0u32), *id)?;
                scores.insert(*id, [].as_slice())?;
                best.insert(*height, *id)?;
                height_idx.remove((101u8, *height))?;
            }
        }
        write_txn.commit()?;

        // Update cache.
        if let Some((height, id)) = entries.last() {
            let mut tip = self.best_header_tip.write().unwrap_or_else(|e| e.into_inner());
            *tip = Some((*height, *id));
        }

        Ok(())
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
        if height > 0 {
            let mut table = write_txn.open_table(HEIGHT_INDEX)?;
            let _ = table.insert((type_id, height), *id)?;
        }
        write_txn.commit()?;

        if height > 0 {
            let mut tips = self.tips.write().unwrap_or_else(|e| e.into_inner());
            if tips.get(&type_id).is_none_or(|tip| height > tip.0) {
                tips.insert(type_id, (height, *id));
            }
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
                if *height > 0 {
                    let _ = height_idx.insert((*type_id, *height), *id)?;
                }
            }
        }
        write_txn.commit()?;

        let mut tips = self.tips.write().unwrap_or_else(|e| e.into_inner());
        for (type_id, id, height, _) in entries {
            if *height > 0 && tips.get(type_id).is_none_or(|tip| *height > tip.0) {
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

    fn put_header(
        &self,
        id: &[u8; 32],
        height: u32,
        fork: u32,
        score: &[u8],
        data: &[u8],
    ) -> Result<(), Self::Error> {
        let write_txn = self.db.begin_write()?;
        {
            let mut primary = write_txn.open_table(PRIMARY)?;
            primary.insert((101u8, *id), data)?;

            let mut forks = write_txn.open_table(HEADER_FORKS)?;
            forks.insert((height, fork), *id)?;

            let mut scores = write_txn.open_table(HEADER_SCORES)?;
            scores.insert(*id, score)?;

            let mut best = write_txn.open_table(BEST_CHAIN)?;
            if best.get(height)?.is_none() {
                best.insert(height, *id)?;
            }
        }
        write_txn.commit()?;

        // Update cache only if we actually wrote to BEST_CHAIN.
        // We wrote iff no entry existed at this height — which for a new height
        // means fork==0 is the first arrival. For fork>0 the height already had
        // an entry so we skipped the BEST_CHAIN insert.
        if fork == 0 {
            let mut tip = self.best_header_tip.write().unwrap_or_else(|e| e.into_inner());
            if tip.is_none_or(|t| height > t.0) {
                *tip = Some((height, *id));
            }
        }

        Ok(())
    }

    fn put_header_batch(
        &self,
        entries: &[([u8; 32], u32, u32, Vec<u8>, Vec<u8>)],
    ) -> Result<(), Self::Error> {
        let write_txn = self.db.begin_write()?;
        let mut new_best_tip: Option<(u32, [u8; 32])> = None;
        {
            let mut primary = write_txn.open_table(PRIMARY)?;
            let mut forks = write_txn.open_table(HEADER_FORKS)?;
            let mut scores = write_txn.open_table(HEADER_SCORES)?;
            let mut best = write_txn.open_table(BEST_CHAIN)?;

            for (id, height, fork, score, data) in entries {
                primary.insert((101u8, *id), data.as_slice())?;
                forks.insert((*height, *fork), *id)?;
                scores.insert(*id, score.as_slice())?;

                if best.get(*height)?.is_none() {
                    best.insert(*height, *id)?;
                    if new_best_tip.is_none_or(|t| *height > t.0) {
                        new_best_tip = Some((*height, *id));
                    }
                }
            }
        }
        write_txn.commit()?;

        // Update cache with highest new best entry.
        if let Some(new_tip) = new_best_tip {
            let mut tip = self.best_header_tip.write().unwrap_or_else(|e| e.into_inner());
            if tip.is_none_or(|t| new_tip.0 > t.0) {
                *tip = Some(new_tip);
            }
        }

        Ok(())
    }

    fn header_ids_at_height(
        &self,
        height: u32,
    ) -> Result<Vec<([u8; 32], u32)>, Self::Error> {
        let read_txn = self.db.begin_read()?;
        let table = match read_txn.open_table(HEADER_FORKS) {
            Ok(t) => t,
            Err(::redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(StoreError::Table(e)),
        };

        let mut results = Vec::new();
        for result in table.range((height, 0u32)..=(height, u32::MAX))? {
            let (key_guard, value_guard) = result?;
            let (_h, fork) = key_guard.value();
            let id = value_guard.value();
            results.push((id, fork));
        }
        Ok(results)
    }

    fn header_score(
        &self,
        id: &[u8; 32],
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        let read_txn = self.db.begin_read()?;
        let table = match read_txn.open_table(HEADER_SCORES) {
            Ok(t) => t,
            Err(::redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(StoreError::Table(e)),
        };
        let value = table.get(*id)?;
        Ok(value.map(|guard| guard.value().to_vec()))
    }

    fn best_header_at(
        &self,
        height: u32,
    ) -> Result<Option<[u8; 32]>, Self::Error> {
        let read_txn = self.db.begin_read()?;
        let table = match read_txn.open_table(BEST_CHAIN) {
            Ok(t) => t,
            Err(::redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(StoreError::Table(e)),
        };
        let value = table.get(height)?;
        Ok(value.map(|guard| guard.value()))
    }

    fn best_header_tip(&self) -> Result<Option<(u32, [u8; 32])>, Self::Error> {
        let tip = self.best_header_tip.read().unwrap_or_else(|e| e.into_inner());
        Ok(*tip)
    }

    fn switch_best_chain(
        &self,
        demote: &[u32],
        promote: &[(u32, [u8; 32])],
    ) -> Result<(), Self::Error> {
        let write_txn = self.db.begin_write()?;
        {
            let mut best = write_txn.open_table(BEST_CHAIN)?;
            for height in demote {
                best.remove(*height)?;
            }
            for (height, id) in promote {
                best.insert(*height, *id)?;
            }
        }
        write_txn.commit()?;

        // Update cache to highest promoted height.
        let new_tip = promote.iter().max_by_key(|(h, _)| *h).copied();
        if let Some(new_tip) = new_tip {
            let mut tip = self.best_header_tip.write().unwrap_or_else(|e| e.into_inner());
            *tip = Some(new_tip);
        }

        Ok(())
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
    fn height_zero_skips_index_and_tip() {
        let (store, _dir) = test_store();
        let id = test_id(1);

        // Put with real height — establishes height index and tip
        store.put(101, &id, 5, b"original").unwrap();
        assert_eq!(store.get_id_at(101, 5).unwrap(), Some(id));
        assert_eq!(store.tip(101).unwrap(), Some((5, id)));

        // Re-put same (type_id, id) with height=0 and new data
        store.put(101, &id, 0, b"updated").unwrap();

        // Primary data updated
        assert_eq!(store.get(101, &id).unwrap(), Some(b"updated".to_vec()));
        // Height index not clobbered
        assert_eq!(store.get_id_at(101, 5).unwrap(), Some(id));
        // No spurious entry at height 0
        assert_eq!(store.get_id_at(101, 0).unwrap(), None);
        // Tip unchanged
        assert_eq!(store.tip(101).unwrap(), Some((5, id)));
    }

    #[test]
    fn empty_store() {
        let (store, _dir) = test_store();

        assert_eq!(store.tip(101).unwrap(), None);
        assert_eq!(store.get(101, &test_id(1)).unwrap(), None);
        assert_eq!(store.get_id_at(101, 0).unwrap(), None);
        assert!(!store.contains(101, &test_id(1)).unwrap());
    }

    // --- Fork-aware header tests ---

    #[test]
    fn put_header_and_query() {
        let (store, _dir) = test_store();
        let id = test_id(1);
        let score = vec![0x00, 0x01, 0xFF];
        let data = b"header bytes";

        store.put_header(&id, 100, 0, &score, data).unwrap();

        // PRIMARY populated (type_id=101)
        assert_eq!(store.get(101, &id).unwrap(), Some(data.to_vec()));

        // HEADER_FORKS populated
        let ids = store.header_ids_at_height(100).unwrap();
        assert_eq!(ids, vec![(id, 0)]);

        // HEADER_SCORES populated
        assert_eq!(store.header_score(&id).unwrap(), Some(score));

        // BEST_CHAIN populated (first at this height)
        assert_eq!(store.best_header_at(100).unwrap(), Some(id));

        // best_header_tip cache updated
        assert_eq!(store.best_header_tip().unwrap(), Some((100, id)));
    }

    #[test]
    fn multiple_forks_at_same_height() {
        let (store, _dir) = test_store();
        let id_a = test_id(0xAA);
        let id_b = test_id(0xBB);
        let score_a = vec![0x01];
        let score_b = vec![0x02];

        // First header at height 50 — becomes best
        store.put_header(&id_a, 50, 0, &score_a, b"fork0").unwrap();
        // Second header at height 50 — does NOT replace best
        store.put_header(&id_b, 50, 1, &score_b, b"fork1").unwrap();

        // Both queryable via header_ids_at_height, sorted by fork number
        let ids = store.header_ids_at_height(50).unwrap();
        assert_eq!(ids, vec![(id_a, 0), (id_b, 1)]);

        // best_header_at still returns first (fork=0)
        assert_eq!(store.best_header_at(50).unwrap(), Some(id_a));

        // Both have scores
        assert_eq!(store.header_score(&id_a).unwrap(), Some(score_a));
        assert_eq!(store.header_score(&id_b).unwrap(), Some(score_b));

        // Both readable from PRIMARY
        assert_eq!(store.get(101, &id_a).unwrap(), Some(b"fork0".to_vec()));
        assert_eq!(store.get(101, &id_b).unwrap(), Some(b"fork1".to_vec()));
    }

    #[test]
    fn switch_best_chain_single_height() {
        let (store, _dir) = test_store();
        let id_a = test_id(0xAA);
        let id_b = test_id(0xBB);

        store.put_header(&id_a, 10, 0, &[0x01], b"a").unwrap();
        store.put_header(&id_b, 10, 1, &[0x02], b"b").unwrap();

        // Currently best at height 10 is id_a
        assert_eq!(store.best_header_at(10).unwrap(), Some(id_a));

        // Switch: demote height 10, promote id_b at height 10
        store.switch_best_chain(&[10], &[(10, id_b)]).unwrap();

        assert_eq!(store.best_header_at(10).unwrap(), Some(id_b));
        assert_eq!(store.best_header_tip().unwrap(), Some((10, id_b)));
    }

    #[test]
    fn switch_best_chain_multi_height() {
        let (store, _dir) = test_store();

        // Build a best chain of 5 headers at heights 1..=5
        for h in 1..=5u32 {
            let id = test_id(h as u8);
            store.put_header(&id, h, 0, &[h as u8], b"best").unwrap();
        }
        assert_eq!(store.best_header_tip().unwrap(), Some((5, test_id(5))));

        // Fork headers at heights 3..=6 (fork is longer)
        let fork_ids: Vec<[u8; 32]> = (3..=6u32).map(|h| {
            let id = test_id(0xF0 + h as u8);
            store.put_header(&id, h, 1, &[0xF0 + h as u8], b"fork").unwrap();
            id
        }).collect();

        // Demote heights 3, 4, 5 — promote fork at 3, 4, 5, 6
        store.switch_best_chain(
            &[3, 4, 5],
            &[
                (3, fork_ids[0]),
                (4, fork_ids[1]),
                (5, fork_ids[2]),
                (6, fork_ids[3]),
            ],
        ).unwrap();

        // Heights 1-2 unchanged
        assert_eq!(store.best_header_at(1).unwrap(), Some(test_id(1)));
        assert_eq!(store.best_header_at(2).unwrap(), Some(test_id(2)));
        // Heights 3-6 switched
        assert_eq!(store.best_header_at(3).unwrap(), Some(fork_ids[0]));
        assert_eq!(store.best_header_at(4).unwrap(), Some(fork_ids[1]));
        assert_eq!(store.best_header_at(5).unwrap(), Some(fork_ids[2]));
        assert_eq!(store.best_header_at(6).unwrap(), Some(fork_ids[3]));
        // Tip is now height 6
        assert_eq!(store.best_header_tip().unwrap(), Some((6, fork_ids[3])));
    }

    #[test]
    fn put_header_batch_works() {
        let (store, _dir) = test_store();
        let id_a = test_id(0xAA);
        let id_b = test_id(0xBB);

        let entries = vec![
            (id_a, 10, 0, vec![0x01], b"header_a".to_vec()),
            (id_b, 11, 0, vec![0x02], b"header_b".to_vec()),
        ];

        store.put_header_batch(&entries).unwrap();

        // Both in PRIMARY
        assert_eq!(store.get(101, &id_a).unwrap(), Some(b"header_a".to_vec()));
        assert_eq!(store.get(101, &id_b).unwrap(), Some(b"header_b".to_vec()));

        // Both in HEADER_FORKS
        assert_eq!(store.header_ids_at_height(10).unwrap(), vec![(id_a, 0)]);
        assert_eq!(store.header_ids_at_height(11).unwrap(), vec![(id_b, 0)]);

        // Both in HEADER_SCORES
        assert_eq!(store.header_score(&id_a).unwrap(), Some(vec![0x01]));
        assert_eq!(store.header_score(&id_b).unwrap(), Some(vec![0x02]));

        // Both in BEST_CHAIN
        assert_eq!(store.best_header_at(10).unwrap(), Some(id_a));
        assert_eq!(store.best_header_at(11).unwrap(), Some(id_b));

        // Tip is the higher one
        assert_eq!(store.best_header_tip().unwrap(), Some((11, id_b)));
    }

    #[test]
    fn migration_from_height_index() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("migrate.redb");

        // Phase 1: populate old-style HEIGHT_INDEX with type 101 headers.
        {
            let db = Database::create(&path).unwrap();
            let write_txn = db.begin_write().unwrap();
            {
                let mut primary = write_txn.open_table(PRIMARY).unwrap();
                let mut height_idx = write_txn.open_table(HEIGHT_INDEX).unwrap();

                for h in 1..=3u32 {
                    let id = test_id(h as u8);
                    primary.insert((101u8, id), format!("data{h}").as_bytes()).unwrap();
                    height_idx.insert((101u8, h), id).unwrap();
                }
                // Also insert a non-header entry (type 102) that should NOT migrate.
                let other_id = test_id(0xFF);
                primary.insert((102u8, other_id), b"other".as_slice()).unwrap();
                height_idx.insert((102u8, 1), other_id).unwrap();
            }
            write_txn.commit().unwrap();
        }

        // Phase 2: open with RedbModifierStore — triggers migration.
        let store = RedbModifierStore::new(&path).unwrap();

        // New tables populated
        for h in 1..=3u32 {
            let id = test_id(h as u8);
            let ids = store.header_ids_at_height(h).unwrap();
            assert_eq!(ids, vec![(id, 0)], "height {h}");
            assert_eq!(store.best_header_at(h).unwrap(), Some(id));
            // Score is empty placeholder
            assert_eq!(store.header_score(&id).unwrap(), Some(vec![]));
        }

        // best_header_tip is height 3
        assert_eq!(store.best_header_tip().unwrap(), Some((3, test_id(3))));

        // Old (101, *) entries removed from HEIGHT_INDEX
        assert_eq!(store.get_id_at(101, 1).unwrap(), None);
        assert_eq!(store.get_id_at(101, 2).unwrap(), None);
        assert_eq!(store.get_id_at(101, 3).unwrap(), None);

        // Non-header entry (type 102) untouched
        assert_eq!(store.get_id_at(102, 1).unwrap(), Some(test_id(0xFF)));

        // PRIMARY data still accessible
        assert_eq!(store.get(101, &test_id(1)).unwrap(), Some(b"data1".to_vec()));
    }
}
