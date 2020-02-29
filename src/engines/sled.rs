use std::path::Path;

use super::KvsEngine;
use crate::Result;

/// Wrapper of `sled::Db`.
pub struct SledKvsEngine {
    db: sled::Db,
}

impl Clone for SledKvsEngine {
    fn clone(&self) -> Self {
        unimplemented!()
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&self, key: String, value: String) -> Result<()> {
        unimplemented!();

        self.db.insert(key, value.into_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        unimplemented!();

        Ok(self
            .db
            .get(key)?
            .map(|buf| String::from_utf8(buf.to_vec()))
            .transpose()?)
    }

    fn remove(&self, key: String) -> Result<()> {
        unimplemented!();

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
