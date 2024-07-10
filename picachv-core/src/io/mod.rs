use std::fs::{self, File};
use std::io::{BufReader, Write};
use std::path::Path;

use picachv_error::{PicachvError, PicachvResult};
use serde::de::DeserializeOwned;
use serde::Serialize;

#[cfg(all(feature = "fast_bin", feature = "parquet"))]
pub mod parquet;

#[cfg(feature = "json")]
pub trait JsonIO: Serialize + DeserializeOwned {
    fn to_json<P: AsRef<Path>>(&self, path: P) -> PicachvResult<()> {
        let file = File::create(path)?;
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
        let file = File::open(path)?;
        let reader = BufReader::new(file);
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

#[cfg(feature = "json")]
impl<T> JsonIO for T where T: Serialize + DeserializeOwned {}

#[cfg(feature = "fast_bin")]
pub trait BinIo: Serialize + DeserializeOwned {
    fn to_bytes<P: AsRef<Path>>(&self, path: P) -> PicachvResult<()> {
        let mut file = File::create(path)?;
        let bytes = bincode::serialize(self).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to serialize binary: {}", e).into())
        })?;
        file.write_all(&bytes).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to write binary: {}", e).into())
        })?;
        Ok(())
    }

    fn to_byte_array(&self) -> PicachvResult<Vec<u8>> {
        bincode::serialize(self).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to serialize binary: {}", e).into())
        })
    }

    fn from_bytes<P: AsRef<Path>>(path: P) -> PicachvResult<Self> {
        let bytes = fs::read(path).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to read binary: {}", e).into())
        })?;
        let res = bincode::deserialize::<Self>(&bytes).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to deserialize binary: {}", e).into())
        })?;

        Ok(res)
    }

    fn from_byte_array(bytes: &[u8]) -> PicachvResult<Self> {
        bincode::deserialize(bytes).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to deserialize binary: {}", e).into())
        })
    }
}

#[cfg(feature = "fast_bin")]
impl<T> BinIo for T where T: Serialize + DeserializeOwned {}

#[cfg(test)]
mod tests {
    use std::env;
    use std::sync::Arc;

    use super::*;
    use crate::dataframe::{PolicyGuardedColumn, PolicyGuardedDataFrame};
    use crate::io::JsonIO;
    use crate::policy::{Policy, PolicyLabel};

    fn test_df() -> PolicyGuardedDataFrame {
        let df = PolicyGuardedColumn::new(vec![
            Arc::new(Policy::PolicyClean),
            Arc::new(Policy::PolicyDeclassify {
                label: PolicyLabel::PolicyTop.into(),
                next: Policy::PolicyClean.into(),
            }),
        ]);
        PolicyGuardedDataFrame::new(vec![Arc::new(df)])
    }

    #[cfg_attr(all(feature = "fast_bin", feature = "parquet"), test)]
    fn test_parquet_roundabout() {
        let df = test_df();
        let path = env::temp_dir().join("test.parquet");
        let res = df.to_parquet(&path);
        assert!(res.is_ok());

        let df2 = PolicyGuardedDataFrame::from_parquet(&path, &[0], None);
        assert!(df2.is_ok());
        let df2 = df2.unwrap();
        assert_eq!(df, df2);
    }

    #[cfg_attr(feature = "fast_bin", test)]
    fn test_bin_roundabout() {
        let df = test_df();
        let path = env::temp_dir().join("test.bin");
        let res = df.to_bytes(&path);
        assert!(res.is_ok());

        let df2 = PolicyGuardedDataFrame::from_bytes(&path);
        assert!(df2.is_ok());
        let df2 = df2.unwrap();
        assert_eq!(df, df2);
    }

    #[test]
    fn test_json_roundabout() {
        let df = test_df();
        let path = env::temp_dir().join("test.json");
        let res = df.to_json(&path);
        assert!(res.is_ok());

        let df2 = PolicyGuardedDataFrame::from_json(&path);
        assert!(df2.is_ok());
        let df2 = df2.unwrap();
        assert_eq!(df, df2);
    }
}
