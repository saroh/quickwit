use std::sync::Arc;

use anyhow::Context;
use quickwit_actors::AsyncActor;
use quickwit_actors::KillSwitch;
use quickwit_actors::SyncActor;
use quickwit_index_config::IndexConfig;
use quickwit_metastore::Metastore;
use quickwit_metastore::MetastoreUriResolver;
use quickwit_storage::StorageUriResolver;

use crate::actors::Indexer;
use crate::actors::IndexerParams;
use crate::actors::Packager;
use crate::actors::Publisher;
use crate::actors::Uploader;
use crate::actors::router::MailboxWithOffsets;
use crate::models::SplitLabel;

// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

const MEM_BUDGET_IN_BYTES: usize = 500_000_000; // MAKE THIS CONFIGURABLE
pub struct IndexingSupervisor {
    index_id: String,
    metastore: Arc<dyn Metastore>,
    storage_uri_resolver: StorageUriResolver,
}

impl IndexingSupervisor {
    pub fn new(
        index_id: &str,
        metastore: Arc<dyn Metastore>,
        storage_uri_resolver: StorageUriResolver,
    ) -> Self {
        IndexingSupervisor {
            index_id: index_id.to_string(),
            metastore,
            storage_uri_resolver,
        }
    }

    async fn spawn_indexing_pipeline(&self) -> anyhow::Result<()> {

        let index_metadata = self
            .metastore
            .index_metadata(&self.index_id)
            .await
            .with_context(|| format!("Failed to get metadata for index {}", &self.index_id))?;

        let index_storage = self
            .storage_uri_resolver
            .resolve(&index_metadata.index_uri)
            .with_context(|| {
                format!("Failed to find index storage {}", &index_metadata.index_uri)
            })?;

        let kill_switch = KillSwitch::default();

        let publisher = Publisher {
            metastore: self.metastore.clone(),
        };
        let (publisher_mailbox, _publisher_handler) =
            publisher.spawn(3, kill_switch.clone());

        let uploader = Uploader {
            storage: index_storage.clone(),
            metastore: self.metastore.clone(),
            publisher_mailbox,
        };
        let (uploader_mailbox, _uploader_handler) = uploader.spawn(1, kill_switch.clone());

        let packager = Packager { uploader_mailbox };
        let (packager_mailbox, _packager_handler) = packager.spawn(1, kill_switch.clone());

        let indexer_params = IndexerParams {
            index: self.index_id.clone(),
            index_config: Arc::from(index_metadata.index_config.clone()),
            mem_budget_in_bytes: MEM_BUDGET_IN_BYTES,
        };

        let mut mailboxes: Vec<MailboxWithOffsets> = Vec::new();
        for (shard_id, shard_config) in index_metadata.sharding_config.shards().iter().enumerate() {
            let split_label = SplitLabel {
                source: "<fixme>".to_string(), // TODO Fixme
                index: self.index_id.to_string(),
                shard_id,
            };
            let indexer_params = IndexerParams {
                index: self.index_id.clone(),
                index_config: Arc::from(index_metadata.index_config.clone()),
                mem_budget_in_bytes: MEM_BUDGET_IN_BYTES
            };
            let writer: Indexer = Indexer::new(
                indexer_params,
                split_label,
                packager_mailbox.clone(),
            )?;
            let (mailbox, _writer_handle) = writer.spawn(100, kill_switch.clone());
            let mailbox_with_offsets = MailboxWithOffsets {
                mailbox,
                checkpoint: index_metadata.per_shards_checkpoint[shard_id].clone(),
            };
            mailboxes.push(mailbox_with_offsets);
        }

        // let source = build_source(
        //     &campaign.source_config.source_id,
        //     &campaign.source_config.source_params,
        //     &index_metadata.checkpoint,
        //     writer_mailbox,
        // )
        // .await?;
        // source.spawn()?;

        Ok(())
    }
}
