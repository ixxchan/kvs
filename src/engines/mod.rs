pub use self::{kvs::KvStore, sled::SledKvsEngine};

pub trait KvsEngine {}

mod kvs;
mod sled;
