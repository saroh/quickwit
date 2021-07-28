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

use std::{ops::Deref, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};


//TODO make sure we can evolve out of the serialization format

#[derive(Debug, Clone)]
pub struct StaticRoutingConfig(Arc<StaticRoutingConfigInner>);

impl StaticRoutingConfig {
    pub fn routing_key(&self) -> &str {
        &self.0.routing_key
    }

    pub fn shards(&self) -> &[ShardConfig] {
        &self.0.shards[..]
    }
}

impl Default for StaticRoutingConfig {
    fn default() -> Self {
        unimplemented!()
    }
}

impl Serialize for StaticRoutingConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StaticRoutingConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de> {
        let inner = StaticRoutingConfigInner::deserialize(deserializer)?;
        Ok(StaticRoutingConfig(Arc::new(inner)))
    }
}


#[derive(Debug, Serialize, Deserialize)]
struct StaticRoutingConfigInner {
    pub routing_key: String,
    pub shards: Vec<ShardConfig>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ShardConfig {
    pub name: String,
    pub mem_budget_in_bytes: u64,
    pub routing_keys: Vec<String>,
    pub commit_policy: CommitPolicy
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommitPolicy {
    pub max_docs: usize,
    pub timeout: Duration,
}
