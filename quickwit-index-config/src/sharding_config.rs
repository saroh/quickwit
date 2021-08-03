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

// TODO clone is slow

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StaticRoutingConfig {
    SingleTenant {
        shard_config: ShardConfig
    },
    Multitenant {
        tenant_key: String,
        shards: HashMap<ShardId, ShardConfig>,
        tenants: HashMap<TenantId, TenantConfig>
    }
}

impl StaticRoutingConfig {
    pub fn shard_configs(&self) -> Vec<(ShardId, ShardConfig)> {
        match self {
            StaticRoutingConfig::SingleTenant { shard_config } => {
                vec![("default-shard".to_string(), shard_config.clone())]
            },
            StaticRoutingConfig::Multitenant { shards, .. } => {
                shards
                    .iter()
                    .map(|(shard_id, shard_config)| {
                        (shard_id.to_string(), shard_config.clone())
                    })
                    .collect::<Vec<(String, ShardConfig)>>()
            }
        }
    }
}

impl Default for StaticRoutingConfig {
    fn default() -> Self {
        StaticRoutingConfig::SingleTenant {
            shard_config: ShardConfig {
                shard_id: "default-shard".to_string(),
                mem_budget_in_bytes: 3_000_000,
                commit_policy: CommitPolicy::default()
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantConfig {
    pub tenant_id: String,
    pub target_shard: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardConfig {
    pub shard_id: String,
    pub mem_budget_in_bytes: u64,
    pub commit_policy: CommitPolicy
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct CommitPolicy {
    pub max_docs: usize,
    pub timeout: Duration,
}

impl Default for CommitPolicy {
    fn default() -> Self {
        CommitPolicy {
            max_docs: 5_000_000,
            timeout: Duration::from_secs(60 * 30)
        }
    }
}
