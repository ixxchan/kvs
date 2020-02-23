use crate::{
    protocol::{Request, Response},
    Result,
};
use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};

pub struct KvsClient {
    stream: TcpStream,
}

impl KvsClient {
    /// Connect to the address of a server
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        Ok(KvsClient { stream })
    }

    /// Set the value of a string key in the server
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let request = Request::Set { key, value };
        serde_json::to_writer(self.stream.try_clone()?, &request)?;
        self.stream.flush();
        let response = serde_json::from_reader(self.stream.try_clone()?)?;
        match response {
            Response::Ok(_) => Ok(()),
            Response::Err(msg) => Err(failure::err_msg(msg)),
        }
    }

    /// Get the string value of a given string key from the server
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let request = Request::Get { key };
        serde_json::to_writer(self.stream.try_clone()?, &request)?;
        self.stream.flush();
        let response = serde_json::from_reader(self.stream.try_clone()?)?;
        match response {
            Response::Ok(value) => Ok(value),
            Response::Err(msg) => Err(failure::err_msg(msg)),
        }
    }

    /// Remove a given key in the server
    pub fn remove(&mut self, key: String) -> Result<()> {
        let request = Request::Rm { key };
        serde_json::to_writer(self.stream.try_clone()?, &request)?;
        self.stream.flush();
        let response = serde_json::from_reader(self.stream.try_clone()?)?;
        match response {
            Response::Ok(_) => Ok(()),
            Response::Err(msg) => Err(failure::err_msg(msg)),
        }
    }
}
