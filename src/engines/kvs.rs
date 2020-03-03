use chashmap::CHashMap;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::fs::{self, File, OpenOptions};
use std::io::prelude::*;
use std::io::{self, BufReader, BufWriter, Seek, SeekFrom};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::KvsEngine;
use crate::Result;

const COMPACTION_THRESHOLD: u64 = 1024;

/// The key-value database. Log-structured file I/O is used internally for persistant storage.
/// The serialization format is JSON because it is human-readable and the most generally used.
#[derive(Clone)]
pub struct KvStore {
    // index map
    imap: Arc<CHashMap<String, LogIndex>>,
    cache: Arc<CHashMap<String, String>>,
    log_dir: Arc<PathBuf>,
    writer: Arc<Mutex<LogWriter>>,
    //reader: LogReader,
    // number of redundant logs
    dead: Arc<Mutex<u64>>,
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        let len;
        let start_pos;
        {
            let mut writer = self.writer.lock().unwrap();

            start_pos = writer.pos;
            let cmd = Command::Set {
                key: key.clone(),
                value: value.clone(),
            };

            serde_json::to_writer(&mut *writer, &cmd)?;
            writer.flush()?;
            len = writer.pos - start_pos;
        }

        self.cache.insert(key.clone(), value);
        if self
            .imap
            .insert(key, LogIndex::new(start_pos, len))
            .is_some()
        {
            *self.dead.lock().unwrap() += 1;
        }

        // kill zombies
        if *self.dead.lock().unwrap() >= COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(value) = self.cache.get(&key) {
            return Ok(Some(value.clone()));
        }
        match self.imap.get(&key) {
            Some(index) => {
                let mut reader = LogReader::new(File::open(self.log_dir.join("log.json"))?);
                reader.seek(SeekFrom::Start(index.pos))?;
                let reader = reader.take(index.len);
                match serde_json::from_reader(reader)? {
                    Command::Set { key: k, value: v } if key == k => {
                        self.cache.insert(key, v.clone());
                        Ok(Some(v))
                    }
                    c => panic!("inconsistent command {:?}", c),
                }
            }
            None => Ok(None),
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        if self.imap.remove(&key).is_none() {
            return Err(failure::err_msg("Key not found"));
        }
        *self.dead.lock().unwrap() += 1;
        self.cache.remove(&key);

        let cmd = Command::Rm { key };
        let mut writer = self.writer.lock().unwrap();
        serde_json::to_writer(&mut *writer, &cmd)?;
        writer.flush()?;
        Ok(())
    }
}

impl KvStore {
    /// Restores an instance of the database located in some direcotry,
    /// or create a new one if no logs exist in this directory
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = Arc::new(path.into());
        debug!("open KvStore {:?}", path);
        std::fs::create_dir_all(&*path)?;
        let cache = Arc::new(CHashMap::new());
        let f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.join("log.json"))?;
        let writer = Arc::new(Mutex::new(LogWriter::new(f)));
        //let reader = LogReader::new(File::open(path.join("log.json")).unwrap());

        // TODO: save index?

        //        match File::open(path.join("index.json")) {
        //            // restore the in-memory index from the index file if it exists
        //            Ok(idx_file) => imap = Arc::new(serde_json::from_reader(BufReader::new(idx_file))?),
        //            // read the log to restore the database in the memory
        //            Err(e) if e.kind() == ErrorKind::NotFound => KvStore::load_log(&path, &mut imap)?,
        //            Err(e) => return Err(e.into()),
        //        }

        // read the log to restore the database in the memory
        let imap = CHashMap::new();
        KvStore::load_log(&path, &imap)?;

        Ok(KvStore {
            imap: Arc::new(imap),
            cache,
            log_dir: path,
            writer,
            //reader,
            dead: Arc::new(Mutex::new(0)),
        })
    }

    // **DEPRECATED**
    //    fn save_index(&self) -> Result<()> {
    //        let idx_file = File::create(self.log_dir.join("index.json"))?;
    //        serde_json::to_writer(idx_file, &self.imap)?;
    //        Ok(())
    //    }

    /// Compacting the log.
    pub fn compact(&self) -> Result<()> {
        let f = File::create(self.log_dir.join("compacted.json")).map_err(|e| {
            failure::err_msg(format!(
                "Fail to compact 1: {}\nself.log_dir: {:?}",
                e, self.log_dir
            ))
        })?;
        let mut compacted_writer = LogWriter::new(f);
        let imap = (*self.imap).clone();
        for (_, mut index) in imap.into_iter() {
            // It seems inefficient to create a reader in every iteration
            let mut reader = LogReader::new(
                File::open(self.log_dir.join("log.json"))
                    .map_err(|e| failure::err_msg(format!("Fail to compact 2: {}", e)))?,
            );
            reader.seek(SeekFrom::Start(index.pos))?;
            let mut reader = reader.take(index.len);
            index.pos = compacted_writer.pos;
            io::copy(&mut reader, &mut compacted_writer)?;
        }

        // close file handlers
        let mut writer = self.writer.lock().unwrap();
        mem::drop(compacted_writer);
        *writer = LogWriter::new(tempfile::tempfile()?);
        // replace the original log with the compacted log
        fs::rename(
            self.log_dir.join("compacted.json"),
            self.log_dir.join("log.json"),
        )?;
        // restore self.writer
        let f = OpenOptions::new()
            .append(true)
            .open(self.log_dir.join("log.json"))?;
        *writer = LogWriter::new(f);
        Ok(())
    }

    fn load_log(path: &Path, map: &CHashMap<String, LogIndex>) -> Result<()> {
        let mut reader = LogReader::new(File::open(path.join("log.json"))?);
        loop {
            let start_pos = reader.pos;
            match Deserializer::from_reader(&mut reader)
                .into_iter::<Command>()
                .next()
            {
                Some(cmd) => {
                    let cmd = cmd?;
                    let len = reader.pos - start_pos;
                    match cmd {
                        Command::Set { key, .. } => {
                            map.insert(key, LogIndex::new(start_pos, len));
                        }
                        Command::Rm { key } => {
                            map.remove(&key);
                        }
                    }
                }
                None => return Ok(()),
            }
        }
    }
}

// **DEPRECATED**
//impl Drop for KvStore {
//    fn drop(&mut self) {
//        if self.save_index().is_err() {
//            // fail to save index
//            let _ = fs::remove_file(self.log_dir.join("index.json"));
//        }
//    }
//}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Rm { key: String },
}

// Record the reading position which is used when loading log file
struct LogReader {
    reader: BufReader<File>,
    pos: u64,
}

impl LogReader {
    fn new(file: File) -> Self {
        let mut reader = BufReader::new(file);
        let pos = reader.seek(SeekFrom::Start(0)).unwrap();
        LogReader { reader, pos }
    }
}

impl Read for LogReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl Seek for LogReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let pos = self.reader.seek(pos)?;
        self.pos = pos;
        Ok(pos)
    }
}

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

#[derive(Serialize, Deserialize, Debug, Clone)]
struct LogIndex {
    pos: u64,
    len: u64,
}

impl LogIndex {
    fn new(pos: u64, len: u64) -> Self {
        LogIndex { pos, len }
    }
}
