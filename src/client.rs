use serde::Deserialize;
use serde_json::de::IoRead;
use serde_json::Deserializer;
use std::io::prelude::*;
use std::net::{TcpStream, ToSocketAddrs};

use crate::protocol::{Request, Response};
use crate::Result;

/// K-V store client.
pub struct KvsClient {
    reader: Deserializer<IoRead<TcpStream>>,
    writer: TcpStream,
}

impl KvsClient {
    /// Connect to the address of a server.
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let writer = TcpStream::connect(addr)?;
        let reader = Deserializer::from_reader(writer.try_clone()?);
        debug!("Connected to {}", writer.peer_addr()?);
        Ok(KvsClient { reader, writer })
    }

    /// Set the value of a string key in the server.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Set { key, value })?;
        self.writer.flush()?;
        match Response::deserialize(&mut self.reader)? {
            Response::Ok(None) => Ok(()),
            Response::Ok(Some(v)) => Err(failure::err_msg(format!("Unexpected response: {:}", v))),
            Response::Err(msg) => Err(failure::err_msg(msg)),
        }
    }

    /// Get the string value of a given string key from the server.
    ///
    /// Returns `Ok(None)` if the key is not found.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        serde_json::to_writer(&mut self.writer, &Request::Get { key })?;
        self.writer.flush()?;
        match Response::deserialize(&mut self.reader)? {
            Response::Ok(value) => Ok(value),
            Response::Err(msg) => Err(failure::err_msg(msg)),
        }
    }

    /// Remove a given key in the server.
    ///
    /// Returns error if the key is not found.
    pub fn remove(&mut self, key: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Rm { key })?;
        self.writer.flush()?;
        match Response::deserialize(&mut self.reader)? {
            Response::Ok(None) => Ok(()),
            Response::Ok(Some(v)) => Err(failure::err_msg(format!("Unexpected response: {:}", v))),
            Response::Err(msg) => Err(failure::err_msg(msg)),
        }
    }
}
