use crate::actor::MessageProcessError;
use crate::actor_handle::{ActorHandle, ActorTermination};
use crate::Mailbox;
use crate::{Actor, ActorMessage, KillSwitch, Progress};
use async_trait::async_trait;
use tokio::sync::watch;
use tokio::time::timeout;

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
        progress: &Progress,
    ) -> Result<(), MessageProcessError>;

    #[doc(hidden)]
    fn spawn(
        self,
        message_queue_limit: usize,
        kill_switch: KillSwitch,
    ) -> (
        Mailbox<Self::Message>,
        ActorHandle<Self::Message, Self::ObservableState>,
    ) {
        let (sender, receiver) = flume::bounded::<ActorMessage<Self::Message>>(message_queue_limit);
        let (state_tx, state_rx) = watch::channel(self.observable_state());
        let actor_name = self.name();
        let progress = Progress::default();
        let default_msg = self.default_message();
        let join_handle = tokio::spawn(async_actor_loop(
            self,
            receiver,
            state_tx,
            kill_switch.clone(),
            progress.clone(),
            default_msg
        ));
        let mailbox = Mailbox::new(sender, actor_name);
        let actor_handle = ActorHandle::new(
            mailbox.clone(),
            state_rx,
            join_handle,
            progress,
            kill_switch,
        );
        (mailbox, actor_handle)
    }
}

async fn try_recv_msg<M: Clone>(
    inbox: &mut flume::Receiver<ActorMessage<M>>,
    default_msg_opt: Option<&M>
) -> Result<Option<ActorMessage<M>>, ActorTermination> {
    if let Some(default_msg) = default_msg_opt {
        match inbox.try_recv() {
            Ok(msg) => Ok(Some(msg)),
            Err(flume::TryRecvError::Disconnected) =>
                Err(ActorTermination::Disconnect),
            Err(flume::TryRecvError::Empty) =>
                Ok(Some(ActorMessage::Message(default_msg.clone())))
        }
    } else {
        match timeout(crate::HEARTBEAT.mul_f32(0.2), inbox.recv_async()).await {
            Ok(Ok(msg)) =>
                Ok(Some(msg)),
            Ok(Err(_recv_error)) =>
                Err(ActorTermination::Disconnect),
            Err(_timeout_error) => Ok(None)
        }
    }
}

async fn async_actor_loop<A: AsyncActor>(
    mut actor: A,
    mut inbox: flume::Receiver<ActorMessage<A::Message>>,
    state_tx: watch::Sender<A::ObservableState>,
    kill_switch: KillSwitch,
    progress: Progress,
    default_msg_opt: Option<A::Message>,
) -> ActorTermination {
    loop {
        if !kill_switch.is_alive() {
            return ActorTermination::KillSwitch;
        }
        progress.record_progress();
        let async_msg_opt =
            match try_recv_msg(&mut inbox, default_msg_opt.as_ref()).await {
                Ok(async_msg_opt) => async_msg_opt,
                Err(actor_termination) => {
                    return actor_termination;
                }
            };
        progress.record_progress();
        if !kill_switch.is_alive() {
            return ActorTermination::KillSwitch;
        }
        let async_msg = if let Some(async_msg) = async_msg_opt {
            async_msg
        } else {
            continue;
        };
        match async_msg {
            ActorMessage::Message(message) => {
                match actor.process_message(message, &progress).await {
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
            ActorMessage::Observe(oneshot) => {
                let state = actor.observable_state();
                let _ = state_tx.send(state);
                // We voluntarily ignore the error here. (An error only occurs if the
                // sender dropped its receiver.)
                let _ = oneshot.send(());
            }
        }
    }
}
