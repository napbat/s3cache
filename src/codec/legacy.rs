//! Read-only decoder for the bincode 1 layout earlier releases wrote: fixed-width
//! little-endian integers, `u64` lengths for strings, bytes, sequences and maps, a `u8`
//! tag for `Option` and `bool`, and a `u32` enum variant index. Every legacy format
//! used exactly this configuration, so this is the migration path that lets their
//! stored bytes stay readable without the unmaintained `bincode` crate.
//!
//! Decoding is bounded by the input: every length is checked against the bytes that
//! remain before anything is borrowed, and collection capacity comes from serde's
//! cautious size hints, so a corrupt length cannot drive an allocation.

use std::fmt;

use serde::Deserialize;
use serde::de::{
    self, DeserializeSeed, EnumAccess, IntoDeserializer, MapAccess, SeqAccess, VariantAccess,
    Visitor,
};

/// Why legacy bytes could not be decoded.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Error {
    /// The input ended inside a value.
    Truncated,
    /// Bytes remained after the value.
    Trailing,
    /// A tag, length or text field held an impossible value.
    Invalid(&'static str),
    /// The target type rejected a decoded value.
    Custom(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => f.write_str("legacy record is truncated"),
            Self::Trailing => f.write_str("legacy record has trailing bytes"),
            Self::Invalid(what) => write!(f, "legacy record is invalid: {what}"),
            Self::Custom(message) => write!(f, "legacy record is invalid: {message}"),
        }
    }
}

impl std::error::Error for Error {}

impl de::Error for Error {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self::Custom(message.to_string())
    }
}

/// Decodes exactly one value from legacy bytes; trailing bytes are an error.
pub(crate) fn from_slice<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T, Error> {
    let mut reader = Reader { input: bytes };
    let value = T::deserialize(&mut reader)?;
    if reader.input.is_empty() {
        Ok(value)
    } else {
        Err(Error::Trailing)
    }
}

struct Reader<'de> {
    input: &'de [u8],
}

impl<'de> Reader<'de> {
    fn take(&mut self, len: usize) -> Result<&'de [u8], Error> {
        if len > self.input.len() {
            return Err(Error::Truncated);
        }
        let (head, rest) = self.input.split_at(len);
        self.input = rest;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        self.take(N)?
            .try_into()
            .map_err(|_| Error::Invalid("fixed-width field"))
    }

    fn u8(&mut self) -> Result<u8, Error> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    /// A length prefix, which can never exceed the bytes that remain.
    fn len(&mut self) -> Result<usize, Error> {
        let len = u64::from_le_bytes(self.array()?);
        usize::try_from(len)
            .ok()
            .filter(|len| *len <= self.input.len())
            .ok_or(Error::Truncated)
    }

    fn bytes(&mut self) -> Result<&'de [u8], Error> {
        let len = self.len()?;
        self.take(len)
    }

    fn str(&mut self) -> Result<&'de str, Error> {
        std::str::from_utf8(self.bytes()?).map_err(|_| Error::Invalid("string is not UTF-8"))
    }

    /// bincode 1 writes a `char` as its bare UTF-8 encoding, one to four bytes.
    fn char(&mut self) -> Result<char, Error> {
        let width = match self.input.first() {
            None => return Err(Error::Truncated),
            Some(lead) if lead & 0x80 == 0 => 1,
            Some(lead) if lead & 0xE0 == 0xC0 => 2,
            Some(lead) if lead & 0xF0 == 0xE0 => 3,
            Some(lead) if lead & 0xF8 == 0xF0 => 4,
            Some(_) => return Err(Error::Invalid("char lead byte")),
        };
        let encoded = std::str::from_utf8(self.take(width)?)
            .map_err(|_| Error::Invalid("char is not UTF-8"))?;
        encoded.chars().next().ok_or(Error::Invalid("empty char"))
    }

    fn tag(&mut self, what: &'static str) -> Result<bool, Error> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::Invalid(what)),
        }
    }
}

macro_rules! fixed {
    ($($method:ident => $visit:ident($ty:ty),)*) => {$(
        fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
            visitor.$visit(<$ty>::from_le_bytes(self.array()?))
        }
    )*};
}

impl<'de> de::Deserializer<'de> for &mut Reader<'de> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Error> {
        Err(Error::Invalid("the legacy layout is not self-describing"))
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_bool(self.tag("bool")?)
    }

    fixed! {
        deserialize_i8 => visit_i8(i8),
        deserialize_i16 => visit_i16(i16),
        deserialize_i32 => visit_i32(i32),
        deserialize_i64 => visit_i64(i64),
        deserialize_i128 => visit_i128(i128),
        deserialize_u8 => visit_u8(u8),
        deserialize_u16 => visit_u16(u16),
        deserialize_u32 => visit_u32(u32),
        deserialize_u64 => visit_u64(u64),
        deserialize_u128 => visit_u128(u128),
        deserialize_f32 => visit_f32(f32),
        deserialize_f64 => visit_f64(f64),
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_char(self.char()?)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_borrowed_str(self.str()?)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_borrowed_bytes(self.bytes()?)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        if self.tag("option tag")? {
            visitor.visit_some(self)
        } else {
            visitor.visit_none()
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let remaining = self.len()?;
        visitor.visit_seq(Items {
            reader: self,
            remaining,
        })
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_seq(Items {
            reader: self,
            remaining: len,
        })
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Error> {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let remaining = self.len()?;
        visitor.visit_map(Items {
            reader: self,
            remaining,
        })
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        self.deserialize_tuple(fields.len(), visitor)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Error> {
        Err(Error::Invalid("the legacy layout has no identifiers"))
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Error> {
        Err(Error::Invalid("the legacy layout cannot skip a value"))
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

/// A counted run of sequence elements or map entries.
struct Items<'a, 'de> {
    reader: &'a mut Reader<'de>,
    remaining: usize,
}

impl<'de> SeqAccess<'de> for Items<'_, 'de> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Error> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        seed.deserialize(&mut *self.reader).map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.remaining)
    }
}

impl<'de> MapAccess<'de> for Items<'_, 'de> {
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Error> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        seed.deserialize(&mut *self.reader).map(Some)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, Error> {
        seed.deserialize(&mut *self.reader)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.remaining)
    }
}

impl<'de> EnumAccess<'de> for &mut Reader<'de> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self), Error> {
        let index = self.u32()?;
        let value = seed.deserialize(index.into_deserializer())?;
        Ok((value, self))
    }
}

impl<'de> VariantAccess<'de> for &mut Reader<'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Error> {
        Ok(())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value, Error> {
        seed.deserialize(self)
    }

    fn tuple_variant<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, Error> {
        de::Deserializer::deserialize_tuple(self, len, visitor)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        de::Deserializer::deserialize_tuple(self, fields.len(), visitor)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bincode::Options as _;
    use bytes::Bytes;
    use serde::{Deserialize, Serialize};

    use super::{Error, from_slice};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum Shape {
        Unit,
        Newtype(i64),
        Tuple(u16, char),
        Struct { flag: bool, ratio: f64 },
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Everything {
        small: (i8, u8, i16, u16, i32, u32),
        wide: (i64, u64, i128, u128, f32),
        chars: Vec<char>,
        text: String,
        /// Written through `serialize_bytes`, the path the warm body took.
        body: Bytes,
        missing: Option<String>,
        present: Option<Vec<u32>>,
        map: HashMap<String, String>,
        shapes: Vec<Shape>,
        unit: (),
    }

    fn everything() -> Everything {
        Everything {
            small: (-8, 200, -16_000, 60_000, -2_000_000_000, 4_000_000_000),
            wide: (i64::MIN, u64::MAX, -(1 << 100), 1 << 120, 1.5),
            chars: vec!['a', 'é', '€', '🦀'],
            text: "a/b weird\0key".to_owned(),
            body: Bytes::from_static(&[0, 1, 2, 255]),
            missing: None,
            present: Some(vec![1, 2, 3]),
            map: HashMap::from([("x-amz-meta-k".to_owned(), "v".to_owned())]),
            shapes: vec![
                Shape::Unit,
                Shape::Newtype(-5),
                Shape::Tuple(7, 'z'),
                Shape::Struct {
                    flag: true,
                    ratio: -0.25,
                },
            ],
            unit: (),
        }
    }

    /// Both bincode 1 entry points the legacy formats used produce the same bytes.
    fn legacy_bytes<T: Serialize>(value: &T) -> Vec<u8> {
        let plain = bincode::serialize(value).unwrap();
        let fixint = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .serialize(value)
            .unwrap();
        assert_eq!(plain, fixint);
        plain
    }

    #[test]
    fn reads_every_shape_bincode_1_wrote() {
        let value = everything();
        assert_eq!(
            from_slice::<Everything>(&legacy_bytes(&value)).unwrap(),
            value
        );
    }

    #[test]
    fn borrows_strings_and_bytes_from_the_input() {
        let bytes = legacy_bytes(&("borrowed", Bytes::from_static(b"tail")));
        let (text, body): (&str, &[u8]) = from_slice(&bytes).unwrap();
        let input = bytes.as_ptr_range();
        assert!(input.contains(&text.as_ptr()));
        assert!(input.contains(&body.as_ptr()));
        assert_eq!((text, body), ("borrowed", &b"tail"[..]));
    }

    #[test]
    fn every_truncation_is_refused() {
        let bytes = legacy_bytes(&everything());
        for cut in 0..bytes.len() {
            assert!(
                from_slice::<Everything>(&bytes[..cut]).is_err(),
                "a record cut at {cut} of {} bytes decoded",
                bytes.len()
            );
        }
    }

    #[test]
    fn trailing_bytes_are_refused() {
        let mut bytes = legacy_bytes(&"key");
        bytes.push(0);
        assert_eq!(from_slice::<String>(&bytes), Err(Error::Trailing));
    }

    #[test]
    fn impossible_tags_and_lengths_are_refused() {
        assert_eq!(
            from_slice::<Option<u8>>(&[2, 0]),
            Err(Error::Invalid("option tag"))
        );
        assert_eq!(from_slice::<bool>(&[2]), Err(Error::Invalid("bool")));
        let mut huge = u64::MAX.to_le_bytes().to_vec();
        huge.extend_from_slice(b"abc");
        assert_eq!(from_slice::<Vec<u8>>(&huge), Err(Error::Truncated));
        assert_eq!(from_slice::<String>(&huge), Err(Error::Truncated));
        let not_utf8 = legacy_bytes(&Bytes::from_static(&[0xFF]));
        assert_eq!(
            from_slice::<String>(&not_utf8),
            Err(Error::Invalid("string is not UTF-8"))
        );
        let unknown_variant = 9_u32.to_le_bytes();
        assert!(matches!(
            from_slice::<Shape>(&unknown_variant),
            Err(Error::Custom(_))
        ));
    }
}
