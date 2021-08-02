use crate::actor::MessageProcessError;
use crate::actor_handle::{ActorHandle, ActorTermination};
use crate::mailbox::{create_mailbox, Capacity, Command, Inbox};
use crate::{Actor, KillSwitch, Progress, ReceptionResult};
use crate::{Mailbox, Message};
use async_trait::async_trait;
use tokio::sync::watch;
use tracing::error;

pub struct Context<'a, M: Message> {
    pub self_mailbox: &'a Mailbox<M>,
    pub progress: &'a Progress,
}

impl<'a, M: Message> Context<'a, M> {
    pub async fn self_send_async(&self, msg: M) {
        if let Err(_send_err) = self.self_mailbox.send_async(msg).await {
            error!("Failed to send error to self. This should never happen.");
        }
    }

    pub fn record_progress(&self) {
        self.progress.record_progress();
    }
}

/// An async actor is executed on a regular tokio task.
///
/// It can make async calls, but it should not block.
/// Actors doing CPU heavy work should implement `SyncActor` instead.
#[async_trait]
pub trait AsyncActor: Actor + Sized {
    /// Processes a message.
    ///
    /// If true is returned, the actors will continue processing messages.
    /// If false is returned, the actor will terminate "gracefully".
    ///
    /// If Err is returned, the actor will be killed, as well as all of the actor
    /// under the same kill switch.
    async fn process_message(
        &mut self,
        message: Self::Message,
        context: Context<'_, Self::Message>,
    ) -> Result<(), MessageProcessError>;

    #[doc(hidden)]
    fn spawn(
        self,
        capacity: Capacity,
        kill_switch: KillSwitch,
    ) -> ActorHandle<Self::Message, Self::ObservableState> {
        let (state_tx, state_rx) = watch::channel(self.observable_state());
        let actor_name = self.name();
        let progress = Progress::default();
        let default_message_opt = self.default_message();
        let (mailbox, inbox) = create_mailbox(actor_name, capacity, default_message_opt);
        let join_handle = tokio::spawn(async_actor_loop(
            self,
            inbox,
            mailbox.clone(),
            state_tx,
            kill_switch.clone(),
            progress.clone(),
        ));
        let actor_handle = ActorHandle::new(
            mailbox.clone(),
            state_rx,
            join_handle,
            progress,
            kill_switch,
        );
        actor_handle
    }
}

async fn async_actor_loop<A: AsyncActor>(
    mut actor: A,
    inbox: Inbox<A::Message>,
    self_mailbox: Mailbox<A::Message>,
    state_tx: watch::Sender<A::ObservableState>,
    kill_switch: KillSwitch,
    progress: Progress,
) -> ActorTermination {
    let mut running = true;
    loop {
        if !kill_switch.is_alive() {
            return ActorTermination::KillSwitch;
        }
        progress.record_progress();
        let reception_result = inbox.try_recv_msg_async(running).await;
        progress.record_progress();
        if !kill_switch.is_alive() {
            return ActorTermination::KillSwitch;
        }
        match reception_result {
            ReceptionResult::Command(cmd) => {
                match cmd {
                    Command::Pause => {
                        running = false;
                    }
                    Command::Stop(cb) => {
                        let _ = cb.send(());
                        return ActorTermination::OnDemand;
                    }
                    Command::Start => {
                        running = true;
                    }
                    Command::Observe(cb) => {
                        let state = actor.observable_state();
                        let _ = state_tx.send(state);
                        // We voluntarily ignore the error here. (An error only occurs if the
                        // sender dropped its receiver.)
                        let _ = cb.send(());
                    }
                }
            }
            ReceptionResult::Message(msg) => {
                let context = Context {
                    self_mailbox: &self_mailbox,
                    progress: &progress,
                };
                match actor.process_message(msg, context).await {
                    Ok(()) => (),
                    Err(MessageProcessError::OnDemand) => return ActorTermination::OnDemand,
                    Err(MessageProcessError::Error(err)) => {
                        kill_switch.kill();
                        return ActorTermination::ActorError(err);
                    }
                    Err(MessageProcessError::DownstreamClosed) => {
                        kill_switch.kill();
                        return ActorTermination::DownstreamClosed;
                    }
                }
            }
            ReceptionResult::None => {
                continue;
            }
            ReceptionResult::Disconnect => {
                return ActorTermination::Disconnect;
            }
        }
    }
}
