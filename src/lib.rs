use failure::Error;
use std::result;

pub use client::KvsClient;
pub use server::KvsServer;
pub use engines::{KvsEngine, KvStore, SledKvsEngine};

mod client;
mod server;
mod engines;

pub type Result<T> = result::Result<T, Error>;
