use quickwit_metastore::Checkpoint;

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

#[derive(Debug)]
pub struct RawBatch {
    pub docs_json: Vec<String>,
    /// Checkpoint marking the last document of the batch.
    ///
    /// Subtle point here:
    ///
    /// For each shard, we will end up using this checkpoint to update the shard's position.
    /// Change of sharding configuration will need to happen on a synchronous manner, meaning
    /// that all shards must be commit right after a given batch before we can change the configuration.
    pub checkpoint_update: Checkpoint,
}
