use std::io::{Read, Result};

use bincode::{config, decode_from_slice};
use byteorder::{LittleEndian, ReadBytesExt};

use crate::{Operation, MAGIC_BYTES};

pub(crate) trait TraceReader {
    fn read(&self) -> Result<Operation>;
    fn read_n(&self, n: usize) -> Result<Vec<Operation>> {
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
    pub(crate) fn new(reader: R) -> Result<Self> {
        assert_eq!(reader.read_u32::<LittleEndian>()?, MAGIC_BYTES);
        Ok(Self { reader })
    }
}

impl<R: ReadBytesExt> TraceReader for TraceReaderImpl<R> {
    fn read(&self) -> Result<Operation> {
        let len = self.reader.read_u64::<LittleEndian>()?;
        const len: u64 = len;
        let mut buf = [0; len];

        self.reader.read_exact(&mut buf)?;

        let (op, size) = decode_from_slice(&buf, config::standard()).unwrap();

        return Ok(op);
    }
}
