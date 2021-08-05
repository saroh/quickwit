use crate::models::IndexedSplit;
use crate::models::PackagedSplit;
use anyhow::Context;
use quickwit_actors::Actor;
use quickwit_actors::Mailbox;
use quickwit_actors::SyncActor;
use quickwit_directories::write_hotcache;
use quickwit_directories::HOTCACHE_FILENAME;
use tantivy::SegmentId;
use tantivy::SegmentMeta;

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

pub struct Packager {
    sink: Mailbox<PackagedSplit>,
}

impl Packager {
    pub fn new(sink: Mailbox<PackagedSplit>) -> Self {
        Packager { sink }
    }
}

impl Actor for Packager {
    type Message = IndexedSplit;

    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {
        ()
    }
}

/// returns true iff merge is required to reach a state where
/// we have zero, or a single segment with no deletes segment.
fn is_merge_required(segment_metas: &[SegmentMeta]) -> bool {
    match &segment_metas {
        // there are no segment to merge
        [] => false,
        // if there is only segment but it has deletes, it
        // still makes sense to merge it alone in order to remove deleted documents.
        [segment_meta] => segment_meta.has_deletes(),
        _ => true,
    }
}

/// Merge all segments of the split into one.
///
/// If there is only one segment and it has no delete, avoid doing anything them.
pub async fn merge_all_segments(split: &mut IndexedSplit) -> anyhow::Result<()> {
    let segment_metas = split.index.searchable_segment_metas()?;
    if !is_merge_required(&segment_metas[..]) {
        return Ok(());
    }
    let segment_ids: Vec<SegmentId> = segment_metas
        .into_iter()
        .map(|segment_meta| segment_meta.id())
        .collect();
    split.index_writer.merge(&segment_ids).await?;
    Ok(())
}

fn commit_split(split: &mut IndexedSplit) -> anyhow::Result<()> {
    split
        .index_writer
        .commit()
        .with_context(|| format!("Commit split `{:?}` failed", &split))?;
    Ok(())
}

/// If necessary, merge all segments and returns a PackagedSplit.
///
/// Note this function implicitly drops the IndexWriter
/// which potentially olds a lot of RAM.
fn merge_segments_in_split(mut split: IndexedSplit) -> anyhow::Result<PackagedSplit> {
    let segment_metas = split.index.searchable_segment_metas()?;
    if is_merge_required(&segment_metas[..]) {
        let segment_ids: Vec<SegmentId> = segment_metas
            .into_iter()
            .map(|segment_meta| segment_meta.id())
            .collect();
        // TODO it would be nice if tantivy could let us run the merge in the current thread.
        futures::executor::block_on(split.index_writer.merge(&segment_ids))?;
    }
    let packaged_split = PackagedSplit {
        split_id: split.split_id,
        temp_dir: split.temp_dir,
    };
    Ok(packaged_split)
}

fn build_hotcache(split: &PackagedSplit) -> anyhow::Result<()> {
    let split_scratch_dir = split.temp_dir.path().to_path_buf();
    let hotcache_path = split_scratch_dir.join(HOTCACHE_FILENAME);
    let mut hotcache_file = std::fs::File::create(&hotcache_path)?;
    let mmap_directory = tantivy::directory::MmapDirectory::open(split_scratch_dir)?;
    write_hotcache(mmap_directory, &mut hotcache_file)?;
    Ok(())
}

impl SyncActor for Packager {
    fn process_message(
        &mut self,
        mut split: IndexedSplit,
        context: quickwit_actors::ActorContext<'_, Self::Message>,
    ) -> Result<(), quickwit_actors::MessageProcessError> {
        commit_split(&mut split)?;
        let mut packaged_split = merge_segments_in_split(split)?;
        build_hotcache(&packaged_split)?;
        Ok(())
    }
}
