// #![deny(missing_docs)]

//! A key-value store

use failure::Error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, prelude::*, BufReader, BufWriter, ErrorKind, SeekFrom};
use std::path::PathBuf;
use std::result;

pub type Result<T> = result::Result<T, Error>;

/// The key-value database. Log-structured file I/O is used internally for persistant storage.
/// The serialization format is JSON because it is human-readable and the most generally used.
pub struct KvStore {
    // index map
    imap: HashMap<String, LogIndex>,
    log_dir: PathBuf,
    writer: LogWriter,
    //reader: LogReader,
}

impl KvStore {
    /// Restores an instance of the database located in some direcotry,
    /// or create a new one if no logs exist in this directory
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        std::fs::create_dir_all(&path)?;
        let mut imap = HashMap::new();
        let f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.join("log.json"))?;
        let writer = LogWriter::new(f);
        //let reader = LogReader::new(File::open(path.join("log.json")).unwrap());

        match File::open(path.join("index.json")) {
            // restore the in-memory index from the index file if it exists
            Ok(idx_file) => imap = serde_json::from_reader(BufReader::new(idx_file))?,
            // read the log to restore the database in the memory
            Err(e) if e.kind() == ErrorKind::NotFound => load_log(),
            Err(e) => Err(e)?,
        }

        Ok(KvStore {
            imap,
            log_dir: path,
            writer,
            //reader,
        })
    }

    /// Set the value of a string key to a string
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let start_pos = self.writer.pos;
        let cmd = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };

        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        let len = self.writer.pos - start_pos;
        self.imap.insert(key, LogIndex::new(start_pos, len));
        Ok(())
    }

    /// Get the string value of a given string key
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.imap.get(&key) {
            Some(index) => {
                let mut reader = LogReader::new(File::open(self.log_dir.join("log.json"))?);
                reader.seek(SeekFrom::Start(index.pos))?;
                let reader = reader.take(index.len);
                match serde_json::from_reader(reader)? {
                    Command::Set { key: k, value: v } if key == k => Ok(Some(v)),
                    _ => panic!("inconsistent command"),
                }
            }
            None => Ok(None),
        }
    }

    /// Remove a given key
    pub fn remove(&mut self, key: String) -> Result<()> {
        if let None = self.imap.remove(&key) {
            return Err(failure::err_msg("Key not found"));
        }

        let cmd = Command::Rm { key: key.clone() };
        serde_json::to_writer(&mut self.writer, &cmd)?;
        Ok(())
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        let idx_file = File::create(self.log_dir.join("index.json")).expect("Fail to create index file");
        serde_json::to_writer(idx_file, &self.imap).expect("Fail to save index file");
    }
}

fn load_log() {
    // unimplemented!();
    // loop {
    //     match Deserializer::from_reader(&mut reader)
    //         .into_iter::<Command>()
    //         .next()
    //     {
    //         Some(cmd) => match cmd? {
    //             Command::Set { key, value } => {
    //                 map.insert(key, value);
    //             }
    //             Command::Rm(key) => {
    //                 map.remove(&key);
    //             }
    //         },
    //         None => break,
    //     }
    //     let mut newline = [0];
    //     if f.read(&mut newline)? < 1 {
    //         break;
    //     }
    //     if newline[0] != b'\n' {
    //         panic!("expected newline");
    //     }
    // }
}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Rm { key: String },
}

type LogReader = BufReader<File>;

// Record the writing position which is used by the index map
struct LogWriter {
    writer: BufWriter<File>,
    pos: u64,
}

impl LogWriter {
    fn new(file: File) -> Self {
        let mut writer = BufWriter::new(file);
        let pos = writer.seek(SeekFrom::End(0)).unwrap();
        LogWriter { writer, pos }
    }
}

impl Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct LogIndex {
    pos: u64,
    len: u64,
}

impl LogIndex {
    fn new(pos: u64, len: u64) -> Self {
        LogIndex { pos, len }
    }
}
