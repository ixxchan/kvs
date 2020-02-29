use std::path::Path;

use super::KvsEngine;
use crate::Result;

/// Wrapper of `sled::Db`.
#[derive(Clone)]
pub struct SledKvsEngine {
    db: sled::Db,
}

impl KvsEngine for SledKvsEngine {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.into_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self
            .db
            .get(key)?
            .map(|buf| String::from_utf8(buf.to_vec()))
            .transpose()?)
    }

    fn remove(&self, key: String) -> Result<()> {
        if self.db.remove(key)?.is_none() {
            Err(failure::err_msg("Key not found"))
        } else {
            self.db.flush()?;
            Ok(())
        }
    }
}

impl SledKvsEngine {
    /// Opens an existed sled instance or creates a new one at the specified path.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(SledKvsEngine {
            db: sled::open(path)?,
        })
    }
}
