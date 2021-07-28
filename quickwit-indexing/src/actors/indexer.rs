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

use anyhow::bail;
use anyhow::Context;
use quickwit_actors::Actor;
use quickwit_actors::Mailbox;
use quickwit_actors::MessageProcessError;
use quickwit_actors::SyncActor;
use quickwit_index_config::IndexConfig;
use quickwit_metastore::Checkpoint;
use tantivy::Index;
use tantivy::IndexWriter;
use tempfile::TempDir;

use crate::models::Batch;
use crate::models::IndexedSplit;
use crate::models::SplitLabel;

struct Scratch {
    index_writer: IndexWriter,
    tempdir: TempDir,
}

impl Scratch {
    pub fn create(
        index_config: &dyn IndexConfig,
        memory_in_bytes: usize,
    ) -> anyhow::Result<Scratch> {
        let tempdir = tempfile::tempdir()?;
        let schema = index_config.schema();
        let index = Index::create_in_dir(tempdir.path(), schema)?;
        let index_writer = index.writer_with_num_threads(1, memory_in_bytes)?;
        Ok(Scratch {
            index_writer,
            tempdir,
        })
    }
}

pub struct IndexerParams {
    pub index: String,
    pub index_config: Arc<dyn IndexConfig>,
    pub mem_budget_in_bytes: usize,
}

#[derive(Default, Clone, Debug)]
pub struct IndexerStatistics {
    num_docs: usize,
}

pub struct Indexer {
    params: IndexerParams,
    packager_mailbox: Mailbox<IndexedSplit>,
    statistics: IndexerStatistics,
    split_label: SplitLabel,
    checkpoint: Checkpoint,
    scratch_opt: Option<Scratch>,
}

impl Indexer {
    pub fn new(
        params: IndexerParams,
        split_label: SplitLabel,
        packager_mailbox: Mailbox<IndexedSplit>,
    ) -> anyhow::Result<Indexer> {
        let mut indexer = Indexer {
            params,
            packager_mailbox,
            statistics: Default::default(),
            scratch_opt: None,
            split_label,
            checkpoint: Checkpoint::default(),
        };
        indexer.create_scratch()?;
        Ok(indexer)
    }

    fn create_scratch(&mut self) -> anyhow::Result<()> {
        if self.scratch_opt.is_some() {
            bail!("Scratch already exists.");
        }
        let scratch = Scratch::create(&*self.params.index_config, self.params.mem_budget_in_bytes)?;
        self.scratch_opt = Some(scratch);
        Ok(())
    }

    fn index_writer(&self) -> anyhow::Result<&IndexWriter> {
        if let Some(scratch) = self.scratch_opt.as_ref() {
            Ok(&scratch.index_writer)
        } else {
            bail!("Missing scratch");
        }
    }
}

impl Actor for Indexer {
    type Message = Batch;

    type ObservableState = IndexerStatistics;

    fn observable_state(&self) -> IndexerStatistics {
        self.statistics.clone()
    }
}

impl SyncActor for Indexer {
    fn process_message(
        &mut self,
        document_batch: Batch,
        _progress: &quickwit_actors::Progress,
    ) -> Result<(), MessageProcessError> {
        let index_writer = self.index_writer()?;
        for doc in document_batch.docs {
            index_writer.add_document(doc);
        }
        self.checkpoint
            .update_checkpoint(document_batch.checkpoint_update);
        Ok(())
    }

    fn finalize(&mut self) -> anyhow::Result<()> {
        let mut scratch = self.scratch_opt.take().with_context(|| "Missing scratch")?;
        scratch.index_writer.commit()?;
        let split = IndexedSplit {
            directory: scratch.tempdir,
            label: self.split_label.clone(),
        };
        self.packager_mailbox.send_blocking(split)?;
        Ok(())
    }
}
