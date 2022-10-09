use bincode::{config, decode_from_std_read};
use byteorder::{LittleEndian, ReadBytesExt};

use crate::error::{Result, TraceError};
use crate::{Operation, MAGIC_BYTES};

pub(crate) trait TraceReader {
    fn read(&mut self) -> Result<Operation>;
    fn read_n(&mut self, n: usize) -> Result<Vec<Operation>> {
        let mut ops = Vec::with_capacity(n);
        for _ in 0..n {
            let op = self.read()?;
            ops.push(op);
        }
        Ok(ops)
    }
}

pub(crate) struct TraceReaderImpl<R: ReadBytesExt> {
    reader: R,
}

impl<R: ReadBytesExt> TraceReaderImpl<R> {
    pub(crate) fn new(mut reader: R) -> Result<Self> {
        let flag = reader.read_u32::<LittleEndian>()?;
        if flag != MAGIC_BYTES {
            Err(TraceError::MagicBytesError {
                expected: MAGIC_BYTES,
                found: flag,
            })
        } else {
            Ok(Self { reader })
        }
    }
}

impl<R: ReadBytesExt> TraceReader for TraceReaderImpl<R> {
    fn read(&mut self) -> Result<Operation> {
        let op = decode_from_std_read(&mut self.reader, config::standard())?;
        return Ok(op);
    }
}

mod test {
    use std::io::Write;

    use bincode::config::{self};
    use bincode::encode_to_vec;
    use byteorder::{LittleEndian, WriteBytesExt};

    use super::{TraceReader, TraceReaderImpl};
    use crate::{MemTraceStore, Operation, MAGIC_BYTES};

    #[test]
    fn read_ops() {
        let count = 5000;
        let mut ops = Vec::new();
        let mut store = MemTraceStore::new();
        store.write_u32::<LittleEndian>(MAGIC_BYTES).unwrap();
        for i in 0..count {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            let op = Operation::Ingest(vec![(key, value)]);
            let buf = encode_to_vec(op.clone(), config::standard()).unwrap();
            store.write(&buf).unwrap();
            ops.push(op);
        }
        let mut reader = TraceReaderImpl::new(store).unwrap();
        for i in 0..count {
            let op = reader.read().unwrap();
            assert_eq!(op, ops[i]);
        }
    }
}
