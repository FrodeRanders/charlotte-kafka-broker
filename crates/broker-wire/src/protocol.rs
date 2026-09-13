//! Protocol constants, error codes, and bounds for the supported subset.

use core::fmt;

/// Kafka API keys implemented by this broker.
pub mod api {
    pub const PRODUCE: i16 = 0;
    pub const FETCH: i16 = 1;
    pub const LIST_OFFSETS: i16 = 2;
    pub const METADATA: i16 = 3;
    pub const API_VERSIONS: i16 = 18;
}

/// The single request/response version accepted for each API.
pub mod version {
    pub const PRODUCE: i16 = 3;
    pub const FETCH: i16 = 4;
    pub const LIST_OFFSETS: i16 = 1;
    pub const METADATA: i16 = 1;
    pub const API_VERSIONS: i16 = 0;
}

/// The subset advertised in ApiVersions responses, as `(api_key, min, max)`.
pub const SUPPORTED_VERSIONS: &[(i16, i16, i16)] = &[
    (api::PRODUCE, version::PRODUCE, version::PRODUCE),
    (api::FETCH, version::FETCH, version::FETCH),
    (api::LIST_OFFSETS, version::LIST_OFFSETS, version::LIST_OFFSETS),
    (api::METADATA, version::METADATA, version::METADATA),
    (api::API_VERSIONS, version::API_VERSIONS, version::API_VERSIONS),
];

pub const NO_ERROR: i16 = 0;
pub const OFFSET_OUT_OF_RANGE: i16 = 1;
pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub const UNSUPPORTED_VERSION: i16 = 35;
pub const UNKNOWN_SERVER_ERROR: i16 = -1;

/// Maximum accepted frame payload length.
pub const MAX_FRAME_LEN: usize = 1024 * 1024;
/// Maximum accepted string length.
pub const MAX_STRING_LEN: usize = 8 * 1024;
/// Maximum accepted array element count.
pub const MAX_ARRAY_LEN: usize = 16 * 1024;
/// Maximum accepted record count per batch.
pub const MAX_RECORDS: usize = 1024;

/// A malformed or unsupported request or response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    /// The frame ended before the declared fields were complete.
    Incomplete,
    /// A field violated a structural rule.
    Invalid,
    /// A field or frame exceeded a bound.
    TooLarge,
    /// The API key is not implemented.
    UnsupportedApi,
    /// The API version is not implemented.
    UnsupportedVersion,
    /// A response correlation id did not match.
    Correlation,
    /// A record batch CRC did not match.
    Checksum,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Incomplete => write!(f, "incomplete frame"),
            Self::Invalid => write!(f, "invalid field"),
            Self::TooLarge => write!(f, "field exceeds the bound"),
            Self::UnsupportedApi => write!(f, "unsupported API"),
            Self::UnsupportedVersion => write!(f, "unsupported API version"),
            Self::Correlation => write!(f, "correlation id mismatch"),
            Self::Checksum => write!(f, "record batch checksum mismatch"),
        }
    }
}

impl core::error::Error for Error {}

/// Whether the API key is part of the supported subset.
pub const fn is_supported_api(api_key: i16) -> bool {
    matches!(
        api_key,
        api::PRODUCE | api::FETCH | api::LIST_OFFSETS | api::METADATA | api::API_VERSIONS
    )
}
