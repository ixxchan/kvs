//! A simple K-V store library which provides pluggable storage engines and
//! client/server functionality.

#[macro_use]
extern crate log;

use failure::Error;
use std::result;

pub use client::KvsClient;
pub use engines::{KvStore, KvsEngine, SledKvsEngine};
pub use server::KvsServer;

mod client;
mod engines;
mod protocol;
mod server;

pub type Result<T> = result::Result<T, Error>;
