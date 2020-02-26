use serde::Deserialize;
use serde_json::de::IoRead;
use serde_json::Deserializer;
use std::io::prelude::*;
use std::net::{TcpStream, ToSocketAddrs};

use crate::protocol::{Request, Response};
use crate::Result;

pub struct KvsClient {
    reader: Deserializer<IoRead<TcpStream>>,
    writer: TcpStream,
}

impl KvsClient {
    /// Connect to the address of a server
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let writer = TcpStream::connect(addr)?;

        let reader = Deserializer::from_reader(writer.try_clone()?);
        debug!("Connected to {}", writer.peer_addr()?);
        Ok(KvsClient { reader, writer })
    }

    /// Set the value of a string key in the server
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let request = Request::Set { key, value };
        serde_json::to_writer(&mut self.writer, &request)?;
        self.writer.flush()?;
        let response: Response = Response::deserialize(&mut self.reader)?;

        match response {
            Response::Ok(_) => Ok(()),
            Response::Err(msg) => Err(failure::err_msg(msg)),
        }
    }

    /// Get the string value of a given string key from the server
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let request = Request::Get { key };
        serde_json::to_writer(&mut self.writer, &request)?;
        self.writer.flush()?;
        debug!(
            "Send request to {}: {:?}",
            self.writer.peer_addr()?,
            request
        );

        let response: Response = Response::deserialize(&mut self.reader)?;

        debug!("Receive response {:?}", response);

        match response {
            Response::Ok(value) => Ok(value),
            Response::Err(msg) => Err(failure::err_msg(msg)),
        }
    }

    /// Remove a given key in the server
    pub fn remove(&mut self, key: String) -> Result<()> {
        let request = Request::Rm { key };
        serde_json::to_writer(&mut self.writer, &request)?;
        self.writer.flush()?;
        let response = Response::deserialize(&mut self.reader)?;
        match response {
            Response::Ok(_) => Ok(()),
            Response::Err(msg) => Err(failure::err_msg(msg)),
        }
    }
}
