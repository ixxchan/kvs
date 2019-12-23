// #![deny(missing_docs)]

//! A key-value store

use failure::Error;
use serde::{Deserialize, Serialize};
use serde_json::{Deserializer};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{prelude::*, SeekFrom};
use std::path::{Path, PathBuf};
use std::result;

pub type Result<T> = result::Result<T, Error>;

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Rm(String),
}

/// The key-value database
pub struct KvStore {
    map: HashMap<String, String>,
    dir: PathBuf,
    writer: File,
}

impl KvStore {
    /// Creates an empty instance of the database
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let mut map = HashMap::new();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(dir.join("log.json"))?;
        // read the log to restore the database in the memory
        loop {
            match Deserializer::from_reader(&mut file)
                .into_iter::<Command>()
                .next()
            {
                Some(cmd) => match cmd? {
                    Command::Set { key, value } => {
                        map.insert(key, value);
                    }
                    Command::Rm(key) => {
                        map.remove(&key);
                    }
                },
                None => break,
            }
            let mut newline = [0];
            if file.read(&mut newline)? < 1 {
                break;
            }
            if newline[0] != b'\n' {
                panic!("expected newline");
            }
        }

        Ok(KvStore {
            map,
            dir: dir.to_owned(),
            writer: file,
        })
    }

    /// Set the value of a string key to a string
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };
        serde_json::to_writer(&mut self.writer, &cmd)?;
        writeln!(&mut self.writer)?;

        self.map.insert(key, value);
        Ok(())
    }

    /// Get the string value of a given string key
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(self.map.get(&key).map(|s| s.to_string()))
    }

    /// Remove a given key
    pub fn remove(&mut self, key: String) -> Result<()> {
        let cmd = Command::Rm(key.clone());
        serde_json::to_writer(&mut self.writer, &cmd)?;
        writeln!(&mut self.writer)?;

        match self.map.remove(&key) {
            Some(_) => Ok(()),
            None => Err(failure::err_msg("Key not found")),
        }
    }
}
