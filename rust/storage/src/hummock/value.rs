use bytes::{Buf, BufMut, Bytes};

use super::{HummockError, HummockResult};

pub const VALUE_DELETE: u8 = 1 << 0;
pub const VALUE_PUT: u8 = 0;

/// [`HummockValue`] can be created on either a `Vec<u8>` or a `&[u8]`.
///
/// Its encoding is a 1-byte flag + user value.
#[derive(Debug, Clone)]
pub enum HummockValue<T> {
    Put(T),
    Delete,
}

impl<T> Copy for HummockValue<T> where T: Copy {}

impl<T: PartialEq> PartialEq for HummockValue<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Put(l0), Self::Put(r0)) => l0.eq(r0),
            (Self::Delete, Self::Delete) => true,
            _ => false,
        }
    }
}

impl<T: Eq> Eq for HummockValue<T> {}

impl<T> HummockValue<T>
where
    T: PartialEq + Eq + AsRef<[u8]>,
{
    pub fn encoded_len(&self) -> usize {
        match self {
            HummockValue::Put(val) => 1 + val.as_ref().len(),
            HummockValue::Delete => 1,
        }
    }

    /// Encode the object
    pub fn encode(&self, buffer: &mut impl BufMut) {
        match self {
            HummockValue::Put(val) => {
                // set flag
                buffer.put_u8(VALUE_PUT);
                buffer.put_slice(val.as_ref());
            }
            HummockValue::Delete => {
                // set flag
                buffer.put_u8(VALUE_DELETE);
            }
        }
    }

    /// Get the put value out of the `HummockValue`. If the current value is `Delete`, `None` will
    /// be returned.
    pub fn into_put_value(self) -> Option<T> {
        match self {
            Self::Put(val) => Some(val),
            Self::Delete => None,
        }
    }
}

impl HummockValue<Vec<u8>> {
    /// Decode the object from `Vec<u8>`.
    pub fn decode(buffer: &mut impl Buf) -> HummockResult<Self> {
        if buffer.remaining() == 0 {
            return Err(HummockError::DecodeError("empty value".to_string()).into());
        }
        match buffer.get_u8() {
            VALUE_PUT => Ok(Self::Put(Vec::from(buffer.chunk()))),
            VALUE_DELETE => Ok(Self::Delete),
            _ => Err(HummockError::DecodeError("non-empty but format error".to_string()).into()),
        }
    }

    pub fn as_slice(&self) -> HummockValue<&[u8]> {
        match self {
            HummockValue::Put(x) => HummockValue::Put(x),
            HummockValue::Delete => HummockValue::Delete,
        }
    }
}

impl<'a> HummockValue<&'a [u8]> {
    /// Decode the object from `&[u8]`.
    pub fn from_slice(mut buffer: &'a [u8]) -> HummockResult<Self> {
        if buffer.remaining() == 0 {
            return Err(HummockError::DecodeError("empty value".to_string()).into());
        }
        match buffer.get_u8() {
            VALUE_PUT => Ok(Self::Put(buffer)),
            VALUE_DELETE => Ok(Self::Delete),
            _ => Err(HummockError::DecodeError("non-empty but format error".to_string()).into()),
        }
    }

    /// Copies `self` into [`HummockValue<Vec<u8>>`].
    pub fn to_owned_value(&self) -> HummockValue<Vec<u8>> {
        match self {
            HummockValue::Put(value) => HummockValue::Put(value.to_vec()),
            HummockValue::Delete => HummockValue::Delete,
        }
    }
}

impl HummockValue<Bytes> {
    pub fn as_slice(&self) -> HummockValue<&[u8]> {
        match self {
            HummockValue::Put(x) => HummockValue::Put(&x[..]),
            HummockValue::Delete => HummockValue::Delete,
        }
    }

    pub fn to_vec(&self) -> HummockValue<Vec<u8>> {
        match self {
            HummockValue::Put(x) => HummockValue::Put(x.to_vec()),
            HummockValue::Delete => HummockValue::Delete,
        }
    }
}

impl From<Option<Vec<u8>>> for HummockValue<Vec<u8>> {
    fn from(data: Option<Vec<u8>>) -> Self {
        match data {
            Some(data) => Self::Put(data),
            None => Self::Delete,
        }
    }
}

impl From<Option<Bytes>> for HummockValue<Bytes> {
    fn from(data: Option<Bytes>) -> Self {
        match data {
            Some(data) => Self::Put(data),
            None => Self::Delete,
        }
    }
}

impl<'a> From<Option<&'a [u8]>> for HummockValue<&'a [u8]> {
    fn from(data: Option<&'a [u8]>) -> Self {
        match data {
            Some(data) => Self::Put(data),
            None => Self::Delete,
        }
    }
}

impl From<HummockValue<Vec<u8>>> for HummockValue<Bytes> {
    fn from(data: HummockValue<Vec<u8>>) -> Self {
        match data {
            HummockValue::Put(x) => HummockValue::Put(x.into()),
            HummockValue::Delete => HummockValue::Delete,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_vec_decode_encode() {
        let mut result = vec![];
        HummockValue::Put(b"233333".to_vec()).encode(&mut result);
        assert_eq!(
            HummockValue::Put(b"233333".to_vec()),
            HummockValue::decode(&mut &result[..]).unwrap()
        );
    }

    #[test]
    fn test_slice_decode_encode() {
        let mut result = vec![];
        HummockValue::Put(b"233333".to_vec()).encode(&mut result);

        assert_eq!(
            HummockValue::Put(b"233333".as_slice()),
            HummockValue::from_slice(&result).unwrap()
        );
    }
}
