use std::path::Path;

use picachv_error::{PicachvError, PicachvResult};
use serde::de::DeserializeOwned;
use serde::Serialize;

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
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let result = serde_json::from_reader(reader).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to read JSON: {}", e).into())
        })?;
        Ok(result)
    }

    fn from_json_bytes(bytes: &[u8]) -> PicachvResult<Self> {
        serde_json::from_slice(bytes).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to read JSON: {}", e).into())
        })
    }
}

impl<T> JsonIO for T where T: Serialize + DeserializeOwned {}
