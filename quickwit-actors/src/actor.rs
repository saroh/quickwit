use std::any::type_name;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tracing::error;

use crate::{Mailbox, QueueCapacity, SendError};

// While the lack of message cannot pause a problem with heartbeating,  sending a message to a saturated channel
// can be interpreted as a blocked actor.

#[derive(Error, Debug)]
pub enum MessageProcessError {
    /// The actor was stopped upon reception of a Command.
    #[error("On Demand")]
    OnDemand,
    /// The actor tried to send a message to a dowstream actor and failed.
    /// The logic ruled that the actor should be killed.
    #[error("Downstream actor closed connection")]
    DownstreamClosed,
    /// Some unexpected error happened.
    #[error("Failure")]
    Error(#[from] anyhow::Error),
    /// The actor terminated, as it identified it reached a state where it
    /// would not send any more message.
    #[error("Terminated")]
    Terminated,
}

impl From<SendError> for MessageProcessError {
    fn from(_: SendError) -> Self {
        MessageProcessError::DownstreamClosed
    }
}

/// An actor has an internal state and processes a stream of message.
///
/// While processing a message, the actor typically
/// - Update its state
/// - emit one or more message to other actors.
///
/// Actors exists in two flavor:
/// - async actors, are executed in event thread in tokio runtime.
/// - sync actors, executed on the blocking thread pool of tokio runtime.
pub trait Actor: Send + Sync + 'static {
    /// Type of message that can be received by the actor.
    type Message: Send + Sync + fmt::Debug;
    /// Piece of state that can be copied for assert in unit test, admin, etc.
    type ObservableState: Send + Sync + Clone + fmt::Debug;
    /// A name identifying the type of actor.
    /// It does not need to be "instance-unique", and can be the name of
    /// the actor implementation.
    fn name(&self) -> String {
        type_name::<Self>().to_string()
    }

    fn default_message(&self) -> Option<Self::Message> {
        None
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Unbounded
    }

    /// Extracts an observable state. Useful for unit test, and admin UI.
    ///
    /// This function should return fast, but it is not called after receiving
    /// single message. Snapshotting happens when the actor is terminated, or
    /// in an on demand fashion by calling `ActorHandle::observe()`.
    fn observable_state(&self) -> Self::ObservableState;
}

/// Makes it possible to register some progress.
///
/// If no progress is observed until the next heartbeat, the actor will be killed.
#[derive(Clone)]
pub struct Progress(Arc<AtomicU8>);

#[repr(u8)]
#[derive(Clone, Debug, Copy)]
enum ProgressState {
    // No update recorded since the last call to .check_for_update()
    NoUpdate = 0,
    // An update was recorded since the last call to .check_for_update()
    Updated = 1,
    // The actor is in the protected zone.
    //
    // The protected zone should seldom be used. It is useful
    // when calling an external library that is blocking for instance.
    //
    // Another use case is blocking when sending a message to another actor
    // with a saturated message bus.
    // The failure detection is then considered to be the problem of
    // the downstream actor.
    //
    // As long as the actor is in the protected zone, healthchecking won't apply
    // to it.
    ProtectedZone = 2,
}

impl Default for Progress {
    fn default() -> Progress {
        Progress(Arc::new(AtomicU8::new(ProgressState::Updated as u8)))
    }
}

impl Progress {
    pub fn record_progress(&self) {
        self.0
            .fetch_max(ProgressState::Updated as u8, Ordering::Relaxed);
    }

    pub fn protect_zone(&self) -> ProtectZoneGuard {
        self.0
            .store(ProgressState::ProtectedZone as u8, Ordering::SeqCst);
        ProtectZoneGuard(self.0.clone())
    }

    /// This method mutates the state as follows and returns true if
    /// the object was in the protected zone or had change registered.
    /// - Updated -> NoUpdate, returns true
    /// - NoUpdate -> Updated, returns true
    /// - ProtectedZone -> ProtectedZone, returns true
    pub fn harvest_changes(&self) -> bool {
        let previous_state = self
            .0
            .compare_exchange(
                ProgressState::Updated as u8,
                ProgressState::NoUpdate as u8,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .unwrap_or_else(|previous_value| previous_value);
        previous_state != ProgressState::NoUpdate as u8
    }
}

pub struct ProtectZoneGuard(Arc<AtomicU8>);

impl Drop for ProtectZoneGuard {
    fn drop(&mut self) {
        self.0.store(ProgressState::Updated as u8, Ordering::SeqCst)
    }
}

#[derive(Clone)]
pub struct KillSwitch {
    alive: Arc<AtomicBool>,
}

impl Default for KillSwitch {
    fn default() -> Self {
        KillSwitch {
            alive: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl KillSwitch {
    pub fn kill(&self) {
        self.alive.store(false, Ordering::Relaxed);
    }

    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }
}
pub struct ActorContext<'a, Message> {
    pub self_mailbox: &'a Mailbox<Message>,
    pub progress: &'a Progress,
    pub kill_switch: &'a KillSwitch,
}

impl<'a, Message> ActorContext<'a, Message> {
    pub async fn self_send_async(&self, msg: Message) {
        if let Err(_send_err) = self.self_mailbox.send_async(msg).await {
            error!("Failed to send error to self. This should never happen.");
        }
    }

    pub fn record_progress(&self) {
        self.progress.record_progress();
    }
}

#[cfg(test)]
mod tests {
    use crate::Progress;

    use super::KillSwitch;

    #[test]
    fn test_kill_switch() {
        let kill_switch = KillSwitch::default();
        assert_eq!(kill_switch.is_alive(), true);
        kill_switch.kill();
        assert_eq!(kill_switch.is_alive(), false);
        kill_switch.kill();
        assert_eq!(kill_switch.is_alive(), false);
    }

    #[test]
    fn test_progress() {
        let progress = Progress::default();
        assert!(progress.harvest_changes());
        progress.record_progress();
        assert!(progress.harvest_changes());
        assert!(!progress.harvest_changes());
    }

    #[test]
    fn test_progress_protect_zone() {
        let progress = Progress::default();
        assert!(progress.harvest_changes());
        progress.record_progress();
        assert!(progress.harvest_changes());
        {
            let _protect_guard = progress.protect_zone();
            assert!(progress.harvest_changes());
            assert!(progress.harvest_changes());
        }
        assert!(progress.harvest_changes());
        assert!(!progress.harvest_changes());
    }
}
