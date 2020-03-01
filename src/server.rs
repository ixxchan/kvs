use serde_json::Deserializer;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

use crate::protocol::{Request, Response};
use crate::thread_pool::*;
use crate::{KvsEngine, Result};

/// K-V store server.
pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    /// Create a `KvsServer` with given store engine
    pub fn new(engine: E, pool: P) -> Self {
        KvsServer { engine, pool }
    }

    pub fn run<A: ToSocketAddrs>(&mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        info!("KvsServer: start working!");
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let engine = self.engine.clone();
                    self.pool.spawn(|| {
                        if let Err(e) = handle_client(engine, stream) {
                            error!("Error when serving client: {}", e);
                        }
                    })
                }
                Err(e) => error!("Connection failed: {}", e),
            }
        }
        Ok(())
    }
}

fn handle_client<E: KvsEngine>(engine: E, stream: TcpStream) -> Result<()> {
    let mut writer = stream.try_clone()?;
    let peer_addr = stream.peer_addr()?;
    debug!("Connected to {}", peer_addr);

    for request in Deserializer::from_reader(stream).into_iter::<Request>() {
        let request = request?;
        debug!("Receive request from {}: {:?}", peer_addr, request);
        let response = match request {
            Request::Get { key } => match engine.get(key) {
                Ok(value) => Response::Ok(value),
                Err(e) => Response::Err(format!("{}", e)),
            },
            Request::Set { key, value } => match engine.set(key, value) {
                Ok(()) => Response::Ok(None),
                Err(e) => Response::Err(format!("{}", e)),
            },
            Request::Rm { key } => match engine.remove(key) {
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
