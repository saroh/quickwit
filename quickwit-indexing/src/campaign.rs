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

use std::sync::Arc;

use quickwit_actors::AsyncActor;
use quickwit_actors::KillSwitch;
use quickwit_actors::SyncActor;
use quickwit_metastore::Metastore;
use quickwit_storage::Storage;

use crate::sources::build_source;
use crate::actors::Indexer;
use crate::actors::IndexerParams;
use crate::actors::Packager;
use crate::actors::Publisher;
use crate::actors::Uploader;
use crate::models::SplitLabel;

const MEM_BUDGET_IN_BYTES: usize = 2_000_000_000;

pub struct Campaign {
    source_config: SourceIndexingConfig,
    split_label: SplitLabel,
    storage: Arc<dyn Storage>,
    metastore: Arc<dyn Metastore>,
}

pub async fn run_campaign(campaign: Campaign) -> anyhow::Result<()> {
    let index_metadata = campaign
        .metastore
        .index_metadata(&campaign.split_label.index)
        .await?;

    let campaign_kill_switch = KillSwitch::default();

    let publisher = Publisher {
        metastore: campaign.metastore.clone(),
    };
    let (publisher_mailbox, _publisher_handler) = publisher.spawn(3, campaign_kill_switch.clone());

    let uploader = Uploader {
        storage: campaign.storage.clone(),
        metastore: campaign.metastore.clone(),
        publisher_mailbox,
    };
    let (uploader_mailbox, _uploader_handler) = uploader.spawn(1, campaign_kill_switch.clone());

    let packager = Packager { uploader_mailbox };
    let (packager_mailbox, _packager_handler) = packager.spawn(1, campaign_kill_switch.clone());

    let indexer_params = IndexerParams {
        index: campaign.split_label.index.clone(),
        index_config: Arc::from(index_metadata.index_config),
        mem_budget_in_bytes: MEM_BUDGET_IN_BYTES,
    };
    let writer: Indexer = Indexer::new(
        indexer_params,
        campaign.split_label.clone(),
        packager_mailbox,
    )?;
    let (writer_mailbox, _writer_handle) = writer.spawn(100, campaign_kill_switch.clone());

    let source = build_source(
        &campaign.source_config.source_id,
        &campaign.source_config.source_params,
        &index_metadata.checkpoint,
        writer_mailbox,
    )
    .await?;
    source.spawn()?;

    Ok(())
}
