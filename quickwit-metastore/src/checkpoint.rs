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

use anyhow::bail;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::{HashMap, hash_map::Entry}};

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Checkpoint {
    per_partition_position: HashMap<String, Vec<u8>>,
}

impl Checkpoint {
    pub fn is_strictly_before(&self, other: &Self) -> bool {
        for (partition_id, offset) in &other.per_partition_position {
            if let Some(original_checkpoint) = self.per_partition_position.get(partition_id) {
                match original_checkpoint[..].cmp(&offset[..]) {
                    Ordering::Less | Ordering::Equal => {
                        return false;
                    },
                    Ordering::Greater => {},
                }
            }
        }
        true
    }

    /// Update the position shipped in the `checkpoint_update`.
    pub fn update_checkpoint(&mut self, checkpoint_update: Checkpoint) -> anyhow::Result<()> {
        if !self.is_strictly_before(&checkpoint_update) {
            bail!("Checkpoint is not allowed");
        }
        for (partition_id, partition_position) in checkpoint_update.per_partition_position {
            self.per_partition_position.insert(partition_id, partition_position);
        }
        Ok(())
    }
}


// TODO tests
