//! Deterministic ownership of connection/session protocol state.
//!
//! Partition logs remain owned by partition shards. A session shard owns only
//! the state that is serialized by one client session (decoder buffer,
//! producer epoch, consumer cursor, and transaction handle as those features
//! are added). Requests still cross an explicit message boundary to the
//! partition shard selected by `(topic, partition)`.

use sitas_core::shard::ShardId;

/// Stable identity for one logical broker session.
///
/// The EL0 listener currently assigns this from its monotonically increasing
/// accepted-connection sequence. A future reconnecting producer may retain a
/// logical identity while advancing its fencing epoch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SessionId(u64);

impl SessionId {
    /// Creates a session identity from a listener-owned sequence number.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the identity's numeric representation for diagnostics.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Logical session-shard layout.
///
/// `first_shard` is an LP/shard hint supplied by the embedding runtime. The
/// layout intentionally remains independent of partition placement: the same
/// session may issue requests for any topic or partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionShardLayout {
    first_shard: usize,
    shard_count: usize,
}

impl SessionShardLayout {
    /// Creates a layout. A zero count is normalized to one so assignment is
    /// total even when a deployment temporarily has no session pool.
    pub const fn new(first_shard: usize, shard_count: usize) -> Self {
        Self {
            first_shard,
            shard_count: if shard_count == 0 {
                1
            } else {
                shard_count
            },
        }
    }

    /// Number of logical session shards.
    pub const fn shard_count(self) -> usize {
        self.shard_count
    }

    /// Deterministically assigns a session to one logical shard.
    pub const fn shard_for(self, session: SessionId) -> ShardId {
        ShardId(self.first_shard + (mix(session.0) as usize % self.shard_count))
    }
}

const fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assignment_is_stable_and_bounded() {
        let layout = SessionShardLayout::new(4, 3);
        for value in 0..100 {
            let shard = layout.shard_for(SessionId::new(value)).0;
            assert!((4..7).contains(&shard));
            assert_eq!(shard, layout.shard_for(SessionId::new(value)).0);
        }
    }

    #[test]
    fn zero_shards_remains_total() {
        assert_eq!(SessionShardLayout::new(2, 0).shard_for(SessionId::new(1)).0, 2);
    }
}
