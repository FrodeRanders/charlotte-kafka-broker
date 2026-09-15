//! Big-endian encoder, decoder, and varint helpers for the wire subset.

use alloc::vec::Vec;

use crate::protocol::{
    Error,
    MAX_ARRAY_LEN,
    MAX_FRAME_LEN,
    MAX_STRING_LEN,
};

/// Builds a length-prefixed Kafka response frame.
pub(crate) struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    /// Starts a response frame with its correlation id.
    pub(crate) fn response(correlation_id: i32) -> Self {
        let mut bytes = Vec::with_capacity(64);
        bytes.extend_from_slice(&0i32.to_be_bytes());
        bytes.extend_from_slice(&correlation_id.to_be_bytes());
        Self {
            bytes,
        }
    }

    pub(crate) fn i8(&mut self, value: i8) {
        self.bytes.push(value as u8);
    }

    pub(crate) fn i16(&mut self, value: i16) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub(crate) fn i32(&mut self, value: i32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub(crate) fn i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub(crate) fn bool(&mut self, value: bool) {
        self.i8(i8::from(value));
    }

    pub(crate) fn string(&mut self, value: &[u8]) -> Result<(), Error> {
        if value.len() > MAX_STRING_LEN || value.len() > i16::MAX as usize {
            return Err(Error::TooLarge);
        }
        self.i16(value.len() as i16);
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    pub(crate) fn nullable_string(&mut self, value: Option<&[u8]>) -> Result<(), Error> {
        match value {
            Some(value) => self.string(value),
            None => {
                self.i16(-1);
                Ok(())
            }
        }
    }

    pub(crate) fn bytes(&mut self, value: &[u8]) -> Result<(), Error> {
        if value.len() > MAX_FRAME_LEN || value.len() > i32::MAX as usize {
            return Err(Error::TooLarge);
        }
        self.i32(value.len() as i32);
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    pub(crate) fn array_len(&mut self, len: usize) -> Result<(), Error> {
        if len > MAX_ARRAY_LEN || len > i32::MAX as usize {
            return Err(Error::TooLarge);
        }
        self.i32(len as i32);
        Ok(())
    }

    pub(crate) fn nullable_array_len(&mut self, len: Option<usize>) -> Result<(), Error> {
        match len {
            Some(len) => self.array_len(len),
            None => {
                self.i32(-1);
                Ok(())
            }
        }
    }

    /// Patches the length prefix and returns the completed frame.
    pub(crate) fn finish(mut self) -> Vec<u8> {
        let payload = self.bytes.len() - 4;
        debug_assert!(payload <= MAX_FRAME_LEN);
        self.bytes[..4].copy_from_slice(&(payload as i32).to_be_bytes());
        self.bytes
    }
}

/// Reads big-endian fields from one frame.
pub(crate) struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    pub(crate) const fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            offset: 0,
        }
    }

    pub(crate) fn take(&mut self, len: usize) -> Result<&'a [u8], Error> {
        let end = self.offset.checked_add(len).ok_or(Error::Invalid)?;
        let value = self.bytes.get(self.offset..end).ok_or(Error::Incomplete)?;
        self.offset = end;
        Ok(value)
    }

    pub(crate) fn i8(&mut self) -> Result<i8, Error> {
        Ok(self.take(1)?[0] as i8)
    }

    pub(crate) fn i16(&mut self) -> Result<i16, Error> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().map_err(|_| Error::Invalid)?))
    }

    pub(crate) fn i32(&mut self) -> Result<i32, Error> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().map_err(|_| Error::Invalid)?))
    }

    pub(crate) fn i64(&mut self) -> Result<i64, Error> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().map_err(|_| Error::Invalid)?))
    }

    pub(crate) fn string_bytes(&mut self) -> Result<&'a [u8], Error> {
        let len = self.i16()?;
        if len < 0 {
            return Err(Error::Invalid);
        }
        let len = len as usize;
        if len > MAX_STRING_LEN {
            return Err(Error::TooLarge);
        }
        self.take(len)
    }

    pub(crate) fn nullable_string_bytes(&mut self) -> Result<Option<&'a [u8]>, Error> {
        let len = self.i16()?;
        if len == -1 {
            return Ok(None);
        }
        if len < 0 {
            return Err(Error::Invalid);
        }
        let len = len as usize;
        if len > MAX_STRING_LEN {
            return Err(Error::TooLarge);
        }
        Ok(Some(self.take(len)?))
    }

    pub(crate) fn bytes(&mut self) -> Result<Option<&'a [u8]>, Error> {
        let len = self.i32()?;
        if len == -1 {
            return Ok(None);
        }
        if len < 0 {
            return Err(Error::Invalid);
        }
        let len = len as usize;
        if len > MAX_FRAME_LEN {
            return Err(Error::TooLarge);
        }
        Ok(Some(self.take(len)?))
    }

    pub(crate) fn array_len(&mut self) -> Result<usize, Error> {
        let len = self.i32()?;
        if len < 0 {
            return Err(Error::Invalid);
        }
        let len = len as usize;
        if len > MAX_ARRAY_LEN {
            return Err(Error::TooLarge);
        }
        Ok(len)
    }

    pub(crate) fn nullable_array_len(&mut self) -> Result<Option<usize>, Error> {
        let len = self.i32()?;
        if len == -1 {
            return Ok(None);
        }
        if len < 0 {
            return Err(Error::Invalid);
        }
        let len = len as usize;
        if len > MAX_ARRAY_LEN {
            return Err(Error::TooLarge);
        }
        Ok(Some(len))
    }

    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    pub(crate) fn done(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

pub(crate) fn put_uvarint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

pub(crate) fn put_varint(output: &mut Vec<u8>, value: i32) {
    put_uvarint(output, ((value << 1) ^ (value >> 31)) as u32 as u64);
}

pub(crate) fn put_varlong(output: &mut Vec<u8>, value: i64) {
    put_uvarint(output, ((value << 1) ^ (value >> 63)) as u64);
}

pub(crate) fn read_uvarint(decoder: &mut Decoder<'_>, max_bytes: usize) -> Result<u64, Error> {
    let mut value = 0u64;
    for shift in (0..max_bytes * 7).step_by(7) {
        let byte = decoder.take(1)?[0];
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(Error::Invalid)
}

pub(crate) fn read_varint(decoder: &mut Decoder<'_>) -> Result<i32, Error> {
    let raw = read_uvarint(decoder, 5)? as u32;
    Ok(((raw >> 1) as i32) ^ -((raw & 1) as i32))
}

pub(crate) fn read_varlong(decoder: &mut Decoder<'_>) -> Result<i64, Error> {
    let raw = read_uvarint(decoder, 10)?;
    Ok(((raw >> 1) as i64) ^ -((raw & 1) as i64))
}
