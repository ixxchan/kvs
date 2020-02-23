use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Set { key: String, value: String },
    Rm { key: String },
    Get { key: String },
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Ok(Option<String>),
    Err(String),
}
