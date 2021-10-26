// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashSet;
use std::iter::FromIterator;
use std::mem;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::bail;
use async_trait::async_trait;
use fail::fail_point;
use itertools::Itertools;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, AsyncActor, Mailbox, QueueCapacity};
use quickwit_metastore::{Metastore, SplitMetadata, SplitMetadataAndFooterOffsets, SplitState};
use quickwit_storage::BUNDLE_FILENAME;
use tantivy::chrono::Utc;
use tokio::sync::oneshot::Receiver;
use tracing::{info, info_span, warn, Instrument, Span};

use crate::models::{PackagedSplit, PackagedSplitBatch, PublishOperation, PublisherMessage};
use crate::semaphore::Semaphore;
use crate::split_store::IndexingSplitStore;

pub const MAX_CONCURRENT_SPLIT_UPLOAD: usize = 6;

pub struct Uploader {
    actor_name: &'static str,
    metastore: Arc<dyn Metastore>,
    index_storage: IndexingSplitStore,
    publisher_mailbox: Mailbox<Receiver<PublisherMessage>>,
    concurrent_upload_permits: Semaphore,
    counters: UploaderCounters,
}

impl Uploader {
    pub fn new(
        actor_name: &'static str,
        metastore: Arc<dyn Metastore>,
        index_storage: IndexingSplitStore,
        publisher_mailbox: Mailbox<Receiver<PublisherMessage>>,
    ) -> Uploader {
        Uploader {
            actor_name,
            metastore,
            index_storage,
            publisher_mailbox,
            concurrent_upload_permits: Semaphore::new(MAX_CONCURRENT_SPLIT_UPLOAD),
            counters: Default::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct UploaderCounters {
    pub num_staged_splits: Arc<AtomicU64>,
    pub num_uploaded_splits: Arc<AtomicU64>,
}

impl Actor for Uploader {
    type Message = PackagedSplitBatch;

    type ObservableState = UploaderCounters;

    #[allow(clippy::unused_unit)]
    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(1)
    }

    fn name(&self) -> String {
        self.actor_name.to_string()
    }

    fn message_span(&self, msg_id: u64, batch: &PackagedSplitBatch) -> Span {
        info_span!("", msg_id=&msg_id, split=?batch.split_ids())
    }
}

fn create_split_metadata(split: &PackagedSplit) -> SplitMetadataAndFooterOffsets {
    SplitMetadataAndFooterOffsets {
        split_metadata: SplitMetadata {
            split_id: split.split_id.clone(),
            num_records: split.num_docs as usize,
            time_range: split.time_range.clone(),
            size_in_bytes: split.size_in_bytes,
            split_state: SplitState::New,
            update_timestamp: Utc::now().timestamp(),
            tags: split.tags.clone(),
            demux_num_ops: split.demux_num_ops,
        },
        footer_offsets: split.footer_offsets.clone(),
    }
}

fn make_publish_operation(
    mut packaged_splits_and_metadatas: Vec<(PackagedSplit, SplitMetadata)>,
) -> PublishOperation {
    assert!(!packaged_splits_and_metadatas.is_empty());
    let replaced_split_ids = packaged_splits_and_metadatas
        .iter()
        .flat_map(|(split, _)| split.replaced_split_ids.clone())
        .collect::<HashSet<_>>();
    if packaged_splits_and_metadatas.len() == 1 && replaced_split_ids.is_empty() {
        let (mut packaged_split, split_metadata) = packaged_splits_and_metadatas.pop().unwrap();
        assert_eq!(packaged_split.checkpoint_deltas.len(), 1);
        let checkpoint_delta = packaged_split.checkpoint_deltas.pop().unwrap();
        PublishOperation::PublishNewSplit {
            new_split: split_metadata,
            checkpoint_delta,
            split_date_of_birth: packaged_split.split_date_of_birth,
        }
    } else {
        PublishOperation::ReplaceSplits {
            new_splits: packaged_splits_and_metadatas
                .into_iter()
                .map(|split_and_meta| split_and_meta.1)
                .collect_vec(),
            replaced_split_ids: Vec::from_iter(replaced_split_ids),
        }
    }
}

async fn stage_and_upload_split(
    packaged_split: &PackagedSplit,
    split_store: &IndexingSplitStore,
    metastore: &dyn Metastore,
    counters: UploaderCounters,
) -> anyhow::Result<SplitMetadata> {
    let split_metadata_and_footer_offsets = create_split_metadata(packaged_split);
    let index_id = packaged_split.index_id.clone();
    let split_metadata = split_metadata_and_footer_offsets.split_metadata.clone();
    info!("staging-split");
    metastore
        .stage_split(&index_id, split_metadata_and_footer_offsets)
        .await?;
    counters.num_staged_splits.fetch_add(1, Ordering::SeqCst);
    let bundle_path = packaged_split
        .split_scratch_directory
        .path()
        .join(BUNDLE_FILENAME);

    info!("storing-split");
    split_store
        .store_split(&split_metadata, &bundle_path)
        .await?;
    counters.num_uploaded_splits.fetch_add(1, Ordering::SeqCst);
    Ok(split_metadata)
}

#[async_trait]
impl AsyncActor for Uploader {
    async fn process_message(
        &mut self,
        batch: PackagedSplitBatch,
        ctx: &ActorContext<PackagedSplitBatch>,
    ) -> Result<(), ActorExitStatus> {
        fail_point!("uploader:before");
        let (split_uploaded_tx, split_uploaded_rx) = tokio::sync::oneshot::channel();

        // We send the future to the publisher right away.
        // That way the publisher will process the uploaded split in order as opposed to
        // publishing in the order splits finish their uploading.
        ctx.send_message(&self.publisher_mailbox, split_uploaded_rx)
            .await?;

        // The permit will be added back manually to the semaphore the task after it is finished.
        // This is not a valid usage of protected zone here.
        //
        // Protected zone are supposed to be used when the cause for blocking is
        // outside of the responsability of the current actor.
        // For instance, when sending a message on a downstream actor with a saturated
        // mailbox.
        // This is meant to be fixed with ParallelActors.
        let permit_guard = {
            let _guard = ctx.protect_zone();
            self.concurrent_upload_permits.acquire().await
        };
        let kill_switch = ctx.kill_switch().clone();
        let split_ids = batch.split_ids();
        if kill_switch.is_dead() {
            warn!(split_ids=?split_ids,"Kill switch was activated. Cancelling upload.");
            return Err(ActorExitStatus::Killed);
        }
        let metastore = self.metastore.clone();
        let index_storage = self.index_storage.clone();
        let counters = self.counters.clone();
        let index_id = batch.index_id();
        let span = Span::current();
        tokio::spawn(
            async move {
                fail_point!("uploader:intask:before");
                let mut packaged_splits_and_metadatas = Vec::new();
                for split in batch.into_iter() {
                    let upload_result = stage_and_upload_split(
                        &split,
                        &index_storage,
                        &*metastore,
                        counters.clone(),
                    )
                    .await;
                    if let Err(cause) = upload_result {
                        warn!(cause=%cause, "Failed to upload split. Killing!");
                        kill_switch.kill();
                        bail!("Failed to upload split `{}`. Killing!", split.split_id);
                    }
                    packaged_splits_and_metadatas.push((split, upload_result.unwrap()));
                }
                let operation = make_publish_operation(packaged_splits_and_metadatas);
                let publisher_message = PublisherMessage {
                    index_id,
                    operation,
                };
                if let Err(publisher_message) = split_uploaded_tx.send(publisher_message) {
                    bail!(
                        "Failed to send upload split `{:?}`. The publisher is probably dead.",
                        &publisher_message
                    );
                }
                info!("success-stage-and-store-split");
                // We explicitely drop it in order to force move the permit guard into the async
                // task.
                mem::drop(permit_guard);
                Result::<(), anyhow::Error>::Ok(())
            }
            .instrument(span),
        );
        fail_point!("uploader:intask:after");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Instant;

    use quickwit_actors::{create_test_mailbox, ObservationType, Universe};
    use quickwit_metastore::checkpoint::CheckpointDelta;
    use quickwit_metastore::MockMetastore;
    use quickwit_storage::RamStorage;

    use super::*;
    use crate::models::ScratchDirectory;

    #[tokio::test]
    async fn test_uploader() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();
        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_stage_split()
            .withf(move |index_id, metadata| -> bool {
                (index_id == "test-index")
                    && &metadata.split_metadata.split_id == "test-split"
                    && metadata.split_metadata.time_range == Some(1628203589..=1628203640)
                    && metadata.split_metadata.split_state == SplitState::New
            })
            .times(1)
            .returning(|_, _| Ok(()));
        let ram_storage = RamStorage::default();
        let index_storage: IndexingSplitStore =
            IndexingSplitStore::create_with_no_local_store(Arc::new(ram_storage.clone()));
        let uploader = Uploader::new(
            "TestUploader",
            Arc::new(mock_metastore),
            index_storage,
            mailbox,
        );
        let (uploader_mailbox, uploader_handle) = universe.spawn_actor(uploader).spawn_async();
        let split_scratch_directory = ScratchDirectory::for_test()?;
        std::fs::write(
            split_scratch_directory.path().join(BUNDLE_FILENAME),
            &b"bubu"[..],
        )?;
        universe
            .send_message(
                &uploader_mailbox,
                PackagedSplitBatch::new(vec![PackagedSplit {
                    split_id: "test-split".to_string(),
                    index_id: "test-index".to_string(),
                    checkpoint_deltas: vec![CheckpointDelta::from(3..15)],
                    time_range: Some(1_628_203_589i64..=1_628_203_640i64),
                    size_in_bytes: 1_000,
                    footer_offsets: 1000..2000,
                    split_scratch_directory,
                    num_docs: 10,
                    demux_num_ops: 0,
                    tags: Default::default(),
                    replaced_split_ids: Vec::new(),
                    split_date_of_birth: Instant::now(),
                }]),
            )
            .await?;
        assert_eq!(
            uploader_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        let publish_futures = inbox.drain_available_message_for_test();
        assert_eq!(publish_futures.len(), 1);
        let publish_future = publish_futures.into_iter().next().unwrap();
        let publisher_message = publish_future.await?;
        assert_eq!(&publisher_message.index_id, "test-index");
        if let PublishOperation::PublishNewSplit {
            new_split,
            checkpoint_delta,
            ..
        } = publisher_message.operation
        {
            assert_eq!(&new_split.split_id, "test-split");
            assert_eq!(checkpoint_delta, CheckpointDelta::from(3..15));
        } else {
            panic!("Expected publish new split operation");
        }
        let mut files = ram_storage.list_files().await;
        files.sort();
        assert_eq!(&files, &[PathBuf::from("test-split.split")]);
        Ok(())
    }

    #[tokio::test]
    async fn test_uploader_emits_replace() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();
        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_stage_split()
            .withf(move |index_id, metadata| -> bool {
                (index_id == "test-index")
                    && vec!["test-split-1".to_owned(), "test-split-2".to_owned()]
                        .contains(&metadata.split_metadata.split_id)
                    && metadata.split_metadata.time_range == Some(1628203589..=1628203640)
                    && metadata.split_metadata.split_state == SplitState::New
            })
            .times(2)
            .returning(|_, _| Ok(()));
        let ram_storage = RamStorage::default();
        let index_storage: IndexingSplitStore =
            IndexingSplitStore::create_with_no_local_store(Arc::new(ram_storage.clone()));
        let uploader = Uploader::new(
            "TestUploader",
            Arc::new(mock_metastore),
            index_storage,
            mailbox,
        );
        let (uploader_mailbox, uploader_handle) = universe.spawn_actor(uploader).spawn_async();
        let split_scratch_directory_1 = ScratchDirectory::for_test()?;
        let split_scratch_directory_2 = ScratchDirectory::for_test()?;
        std::fs::write(
            split_scratch_directory_1.path().join(BUNDLE_FILENAME),
            &b"bubu"[..],
        )?;
        std::fs::write(
            split_scratch_directory_2.path().join(BUNDLE_FILENAME),
            &b"bubu"[..],
        )?;
        let packaged_split_1 = PackagedSplit {
            split_id: "test-split-1".to_string(),
            index_id: "test-index".to_string(),
            checkpoint_deltas: vec![CheckpointDelta::from(3..15), CheckpointDelta::from(16..18)],
            time_range: Some(1_628_203_589i64..=1_628_203_640i64),
            size_in_bytes: 1_000,
            footer_offsets: 1000..2000,
            split_scratch_directory: split_scratch_directory_1,
            num_docs: 10,
            demux_num_ops: 1,
            tags: Default::default(),
            replaced_split_ids: vec![
                "replaced-split-1".to_string(),
                "replaced-split-2".to_string(),
            ],
            split_date_of_birth: Instant::now(),
        };
        let package_split_2 = PackagedSplit {
            split_id: "test-split-2".to_string(),
            index_id: "test-index".to_string(),
            checkpoint_deltas: vec![CheckpointDelta::from(3..15), CheckpointDelta::from(16..18)],
            time_range: Some(1_628_203_589i64..=1_628_203_640i64),
            size_in_bytes: 1_000,
            footer_offsets: 1000..2000,
            split_scratch_directory: split_scratch_directory_2,
            num_docs: 10,
            demux_num_ops: 1,
            tags: Default::default(),
            replaced_split_ids: vec![
                "replaced-split-1".to_string(),
                "replaced-split-2".to_string(),
            ],
            split_date_of_birth: Instant::now(),
        };
        universe
            .send_message(
                &uploader_mailbox,
                PackagedSplitBatch::new(vec![packaged_split_1, package_split_2]),
            )
            .await?;
        assert_eq!(
            uploader_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        let publish_futures = inbox.drain_available_message_for_test();
        assert_eq!(publish_futures.len(), 1);
        let publish_future = publish_futures.into_iter().next().unwrap();
        let publisher_message = publish_future.await?;
        assert_eq!(&publisher_message.index_id, "test-index");
        if let PublishOperation::ReplaceSplits {
            new_splits,
            mut replaced_split_ids,
        } = publisher_message.operation
        {
            // Sort first to avoid test failing.
            replaced_split_ids.sort();
            assert_eq!(new_splits.len(), 2);
            assert_eq!(new_splits[0].split_id, "test-split-1");
            assert_eq!(new_splits[1].split_id, "test-split-2");
            assert_eq!(
                &replaced_split_ids,
                &[
                    "replaced-split-1".to_string(),
                    "replaced-split-2".to_string()
                ]
            );
            assert_eq!(new_splits[0].demux_num_ops, 1);
            assert_eq!(new_splits[1].demux_num_ops, 1);
        } else {
            panic!("Expected publish new split operation");
        }
        let mut files = ram_storage.list_files().await;
        files.sort();
        assert_eq!(
            &files,
            &[
                PathBuf::from("test-split-1.split"),
                PathBuf::from("test-split-2.split")
            ]
        );
        Ok(())
    }
}