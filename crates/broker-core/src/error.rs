//! Errors produced by the deterministic broker core.

use core::fmt;

/// A rejected log or catalog operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogError {
    /// The topic is not present in the catalog.
    UnknownTopic,
    /// The partition index is negative or at or beyond the topic's count.
    UnknownPartition,
    /// The topic name is empty or its partition count is zero.
    InvalidTopic,
    /// The catalog already contains the topic name.
    DuplicateTopic,
    /// An append carried no records.
    EmptyAppend,
    /// An append exceeded the per-append record bound.
    TooManyRecords,
    /// A fetch offset was negative or beyond the high watermark.
    OffsetOutOfRange,
}

impl fmt::Display for LogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownTopic => write!(f, "unknown topic"),
            Self::UnknownPartition => write!(f, "unknown partition"),
            Self::InvalidTopic => write!(f, "invalid topic definition"),
            Self::DuplicateTopic => write!(f, "duplicate topic"),
            Self::EmptyAppend => write!(f, "append with no records"),
            Self::TooManyRecords => write!(f, "append exceeds the record bound"),
            Self::OffsetOutOfRange => write!(f, "offset out of range"),
        }
    }
}

impl core::error::Error for LogError {}
