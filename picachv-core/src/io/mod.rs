use std::fs::{self, File};
use std::io::{BufReader, Write};
use std::path::Path;

use picachv_error::{PicachvError, PicachvResult};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::dataframe::{PolicyGuardedDataFrame, PolicyGuardedDataFrameProxy};

#[cfg(all(feature = "fast_bin", feature = "parquet"))]
pub mod parquet;

#[cfg(feature = "json")]
pub trait JsonIO: Sized {
    fn to_json<P: AsRef<Path>>(&self, path: P) -> PicachvResult<()>;

    fn to_json_bytes(&self) -> PicachvResult<Vec<u8>>;

    fn from_json<P: AsRef<Path>>(path: P) -> PicachvResult<Self>;

    fn from_json_bytes(bytes: &[u8]) -> PicachvResult<Self>;
}

#[cfg(feature = "json")]
impl<T> JsonIO for T
where
    T: Serialize + DeserializeOwned,
{
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
impl JsonIO for PolicyGuardedDataFrame {
    #[inline]
    fn from_json<P: AsRef<Path>>(path: P) -> PicachvResult<Self> {
        PolicyGuardedDataFrameProxy::from_json(path).map(Into::into)
    }

    #[inline]
    fn from_json_bytes(bytes: &[u8]) -> PicachvResult<Self> {
        PolicyGuardedDataFrameProxy::from_json_bytes(bytes).map(Into::into)
    }

    #[inline]
    fn to_json<P: AsRef<Path>>(&self, path: P) -> PicachvResult<()> {
        PolicyGuardedDataFrameProxy::from(self).to_json(path)
    }

    #[inline]
    fn to_json_bytes(&self) -> PicachvResult<Vec<u8>> {
        PolicyGuardedDataFrameProxy::from(self).to_json_bytes()
    }
}

#[cfg(feature = "fast_bin")]
pub trait BinIo: Sized {
    fn to_bytes<P: AsRef<Path>>(&self, path: P) -> PicachvResult<()>;

    fn to_byte_array(&self) -> PicachvResult<Vec<u8>>;

    fn from_bytes<P: AsRef<Path>>(path: P) -> PicachvResult<Self>;

    fn from_byte_array(bytes: &[u8]) -> PicachvResult<Self>;
}

#[cfg(feature = "fast_bin")]
impl<T> BinIo for T
where
    T: Serialize + DeserializeOwned,
{
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
impl BinIo for PolicyGuardedDataFrame {
    #[inline]
    fn from_bytes<P: AsRef<Path>>(path: P) -> PicachvResult<Self> {
        PolicyGuardedDataFrameProxy::from_bytes(path).map(Into::into)
    }

    #[inline]
    fn from_byte_array(bytes: &[u8]) -> PicachvResult<Self> {
        PolicyGuardedDataFrameProxy::from_byte_array(bytes).map(Into::into)
    }

    #[inline]
    fn to_bytes<P: AsRef<Path>>(&self, path: P) -> PicachvResult<()> {
        PolicyGuardedDataFrameProxy::from(self).to_bytes(path)
    }

    #[inline]
    fn to_byte_array(&self) -> PicachvResult<Vec<u8>> {
        PolicyGuardedDataFrameProxy::from(self).to_byte_array()
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::sync::Arc;

    use super::*;
    use crate::dataframe::{
        PolicyGuardedColumnProxy, PolicyGuardedDataFrame, PolicyGuardedDataFrameProxy,
    };
    use crate::io::JsonIO;
    use crate::policy::{Policy, PolicyLabel};

    fn test_df() -> PolicyGuardedDataFrame {
        let df = PolicyGuardedColumnProxy::new(vec![
            Arc::new(Policy::PolicyClean),
            Arc::new(Policy::PolicyDeclassify {
                label: PolicyLabel::PolicyTop.into(),
                next: Policy::PolicyClean.into(),
            }),
        ]);
        PolicyGuardedDataFrameProxy { columns: vec![df] }.into()
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

    #[cfg_attr(all(feature = "fast_bin", feature = "parquet"), test)]
    fn test_parquet_read_row_group() {
        let path = "../data/policies/lineitem.parquet.policy.parquet";
        let df = PolicyGuardedDataFrame::from_parquet_row_group(path, &[0], None, 300);
        assert!(df.is_ok_and(|df| {
            println!("{:?}", df.shape());
            df.shape().0 == 2048
        }));
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
