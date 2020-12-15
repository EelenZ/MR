use std::io;

use thiserror::Error;
use failure:: Fail;
#[derive(Debug, Error)]
pub enum MRError {
    #[error("File Not Found")]
    FileNotFound(#[from] io::Error),
}


pub type Result<T> = std::result::Result<T, MRError>;