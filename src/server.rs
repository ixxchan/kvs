use serde_json::Deserializer;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

use crate::protocol::{Request, Response};
use crate::{KvsEngine, Result};

/// K-V store server.
pub struct KvsServer<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> KvsServer<E> {
    /// Create a `KvsServer` with given store engine
    pub fn new(engine: E) -> Self {
        KvsServer { engine }
    }

    pub fn run<A: ToSocketAddrs>(&mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(e) = self.handle_client(stream) {
                        error!("Error when serving client: {}", e);
                    }
                }
                Err(e) => error!("Connection failed: {}", e),
            }
        }
        Ok(())
    }

    fn handle_client(&mut self, stream: TcpStream) -> Result<()> {
        let mut writer = stream.try_clone()?;
        let peer_addr = stream.peer_addr()?;
        debug!("Connected to {}", peer_addr);

        for request in Deserializer::from_reader(stream).into_iter::<Request>() {
            let request = request?;
            debug!("Receive request from {}: {:?}", peer_addr, request);
            let response = match request {
                Request::Get { key } => match self.engine.get(key) {
                    Ok(value) => Response::Ok(value),
                    Err(e) => Response::Err(format!("{}", e)),
                },
                Request::Set { key, value } => match self.engine.set(key, value) {
                    Ok(()) => Response::Ok(None),
                    Err(e) => Response::Err(format!("{}", e)),
                },
                Request::Rm { key } => match self.engine.remove(key) {
                    Ok(()) => Response::Ok(None),
                    Err(e) => Response::Err(format!("{}", e)),
                },
            };
            serde_json::to_writer(&mut writer, &response)?;
            writer.flush()?;
            debug!("Send response to {}: {:?}", peer_addr, response);
        }

        Ok(())
    }
}
