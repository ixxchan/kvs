use super::KvsEngine;
use crate::Result;
use std::path::PathBuf;

pub struct SledKvsEngine {
    db: sled::Db,
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.into_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(self
            .db
            .get(key)?
            .map(|buf| String::from_utf8(buf.to_vec()))
            .transpose()?)
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if let None = self.db.remove(key)? {
            Err(failure::err_msg("Key not found"))
        } else {
            self.db.flush()?;
            Ok(())
        }
    }
}

impl SledKvsEngine {
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        Ok(SledKvsEngine {
            db: sled::open(path)?,
        })
    }
}
