use std::fs;
use std::io::Write;
use std::path::Path;

use picachv_error::{PicachvError, PicachvResult};
use serde::de::DeserializeOwned;
use serde::Serialize;
#[cfg(feature = "fast_bin")]
use speedy::{Readable, Writable};

use crate::dataframe::PolicyGuardedDataFrame;

pub trait JsonIO: Serialize + DeserializeOwned {
    fn to_json<P: AsRef<Path>>(&self, path: P) -> PicachvResult<()> {
        let file = std::fs::File::create(path)?;
        serde_json::to_writer(file, self).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to write JSON: {}", e).into())
        })?;
        Ok(())
    }

    fn to_json_bytes(&self) -> PicachvResult<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to write JSON: {}", e).into())
        })
    }

    fn from_json<P: AsRef<Path>>(path: P) -> PicachvResult<Self> {
        let now = std::time::Instant::now();
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let result = serde_json::from_reader(reader).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to read JSON: {}", e).into())
        })?;
        println!("Time to read JSON: {:?}", now.elapsed());
        Ok(result)
    }

    fn from_json_bytes(bytes: &[u8]) -> PicachvResult<Self> {
        serde_json::from_slice(bytes).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to read JSON: {}", e).into())
        })
    }
}

impl<T> JsonIO for T where T: Serialize + DeserializeOwned {}

#[cfg(feature = "fast_bin")]
pub trait BinIo: Serialize + DeserializeOwned {
    fn to_bytes<P: AsRef<Path>>(&self, path: P) -> PicachvResult<()>;
    fn from_bytes<P: AsRef<Path>>(path: P) -> PicachvResult<Self>;
}

#[cfg(feature = "fast_bin")]
impl BinIo for PolicyGuardedDataFrame {
    fn to_bytes<P: AsRef<Path>>(&self, path: P) -> PicachvResult<()> {
        let mut file = std::fs::File::create(path)?;
        let bytes = self.write_to_vec().map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to serialize binary: {}", e).into())
        })?;
        file.write_all(&bytes).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to write binary: {}", e).into())
        })?;
        Ok(())
    }

    fn from_bytes<P: AsRef<Path>>(path: P) -> PicachvResult<Self> {
        let now = std::time::Instant::now();
        let bytes = fs::read(path).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to read binary: {}", e).into())
        })?;
        let res = PolicyGuardedDataFrame::read_from_buffer(&bytes).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to deserialize binary: {}", e).into())
        })?;

        println!("Time to read binary: {:?}", now.elapsed());
        Ok(res)
    }
}
