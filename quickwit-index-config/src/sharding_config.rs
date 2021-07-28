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
use std::time::Duration;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{ShardId, TenantId};


//TODO make sure we can evolve out of the serialization format

#[derive(Debug, Clone)]
pub struct StaticRoutingConfig(Arc<StaticRoutingConfigInner>);

impl StaticRoutingConfig {
    pub fn tenant_key(&self) -> &str {
        &self.0.tenant_key
    }

    pub fn shards(&self) -> &HashMap<ShardId, ShardConfig> {
        &self.0.shards
    }

    pub fn tenants(&self) -> &HashMap<TenantId, TenantConfig> {
        &self.0.tenants
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
    pub tenant_key: String,
    pub shards: HashMap<ShardId, ShardConfig>,
    pub tenants: HashMap<TenantId, TenantConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TenantConfig {
    pub tenant_id: String,
    pub target_shard: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ShardConfig {
    pub shard_id: String,
    pub mem_budget_in_bytes: u64,
    pub commit_policy: CommitPolicy
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommitPolicy {
    pub max_docs: usize,
    pub timeout: Duration,
}
