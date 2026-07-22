//! Bounded circular buffer for telemetry holdback during network dropouts.
//!
//! When the gRPC connection drops, the agent continues collecting metrics
//! and stores them in this buffer. On reconnection, buffered snapshots are
//! drained and sent before live data resumes.

use std::collections::VecDeque;

use pulse_core::proto::TelemetryMessage;

/// Fixed-capacity ring buffer for telemetry messages.
///
/// When the buffer is full, the oldest entry is evicted to make room
/// for the newest — ensuring bounded memory usage.
pub struct TelemetryBuffer {
    inner: VecDeque<TelemetryMessage>,
    capacity: usize,
}

impl TelemetryBuffer {
    /// Create a new buffer with the given maximum capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    /// Push a telemetry message into the buffer.
    ///
    /// If the buffer is at capacity, the oldest message is discarded.
    pub fn push(&mut self, msg: TelemetryMessage) {
        if self.inner.len() >= self.capacity {
            self.inner.pop_front();
        }
        self.inner.push_back(msg);
    }

    /// Drain all buffered messages in FIFO order.
    pub fn drain(&mut self) -> impl Iterator<Item = TelemetryMessage> + '_ {
        self.inner.drain(..)
    }

    /// Current number of buffered messages.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_msg(id: i64) -> TelemetryMessage {
        use pulse_core::proto::{telemetry_message::Payload, Heartbeat};
        TelemetryMessage {
            payload: Some(Payload::Heartbeat(Heartbeat {
                node_id: "test".into(),
                timestamp_ms: id,
            })),
        }
    }

    #[test]
    fn evicts_oldest_when_full() {
        let mut buf = TelemetryBuffer::new(3);
        buf.push(make_msg(1));
        buf.push(make_msg(2));
        buf.push(make_msg(3));
        buf.push(make_msg(4)); // should evict msg 1

        assert_eq!(buf.len(), 3);
        let drained: Vec<_> = buf.drain().collect();
        assert_eq!(drained.len(), 3);

        // Verify oldest was evicted
        if let Some(pulse_core::proto::telemetry_message::Payload::Heartbeat(hb)) =
            &drained[0].payload
        {
            assert_eq!(hb.timestamp_ms, 2);
        }
    }

    #[test]
    fn drain_empties_buffer() {
        let mut buf = TelemetryBuffer::new(10);
        buf.push(make_msg(1));
        buf.push(make_msg(2));
        let _: Vec<_> = buf.drain().collect();
        assert!(buf.is_empty());
    }
}
