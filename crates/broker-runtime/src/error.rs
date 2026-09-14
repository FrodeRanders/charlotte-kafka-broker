//! Runtime errors surfaced to the broker caller.

use core::fmt;

use broker_core::{
    CoordinationError,
    LogError,
};

/// A failed broker operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerError {
    /// The broker configuration is inconsistent.
    InvalidConfig,
    /// A shard mailbox stayed full or closed for the whole retry budget.
    MailboxUnavailable,
    /// A shard did not answer within the retry budget.
    ReplyTimeout,
    /// A shard answered with a result that does not match the request.
    UnexpectedReply,
    /// The deterministic core rejected the operation.
    Core(LogError),
    /// A transaction or consumer-group state transition was fenced/rejected.
    Coordination(CoordinationError),
}

impl From<LogError> for BrokerError {
    fn from(error: LogError) -> Self {
        Self::Core(error)
    }
}

impl fmt::Display for BrokerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig => write!(f, "invalid broker configuration"),
            Self::MailboxUnavailable => write!(f, "shard mailbox unavailable"),
            Self::ReplyTimeout => write!(f, "shard reply timed out"),
            Self::UnexpectedReply => write!(f, "unexpected shard reply"),
            Self::Core(error) => write!(f, "broker core error: {error}"),
            Self::Coordination(error) => write!(f, "coordination error: {error}"),
        }
    }
}

impl core::error::Error for BrokerError {}
