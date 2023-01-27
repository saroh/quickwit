// Copyright (C) 2022 Quickwit, Inc.
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
use std::net::SocketAddr;

use chitchat::{ClusterStateSnapshot, NodeId, NodeState};
use itertools::Itertools;
use quickwit_proto::indexing_api::IndexingTask;
use serde::{Deserialize, Serialize};
use tracing::{error, warn};

use crate::QuickwitService;

// Keys used to store member's data in chitchat state.
pub(crate) const GRPC_ADVERTISE_ADDR_KEY: &str = "grpc_advertise_addr";
pub(crate) const ENABLED_SERVICES_KEY: &str = "enabled_services";
pub(crate) const RUNNING_INDEXING_PLAN: &str = "running_indexing_plan";

/// Cluster member.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterMember {
    /// A unique node ID across the cluster.
    /// The Chitchat node ID is the concatenation of the node ID and the start timestamp:
    /// `{node_id}/{start_timestamp}`.
    pub node_id: String,
    /// The start timestamp (seconds) of the node.
    pub start_timestamp: u64,
    /// Enabled services, i.e. services configured to run on the node. Depending on the node and
    /// service health, each service may or may not be available/running.
    pub enabled_services: HashSet<QuickwitService>,
    /// Gossip advertise address, i.e. the address that other nodes should use to gossip with the
    /// node.
    pub gossip_advertise_addr: SocketAddr,
    /// gRPC advertise address, i.e. the address that other nodes should use to communicate with
    /// the node via gRPC.
    pub grpc_advertise_addr: SocketAddr,
    /// Running indexing plan.
    /// None if the node is not an indexer or the indexer has not yet started some indexing
    /// pipelines.
    pub running_indexing_plan: Option<RunningIndexingPlan>,
}

impl ClusterMember {
    pub fn new(
        node_id: String,
        start_timestamp: u64,
        enabled_services: HashSet<QuickwitService>,
        gossip_advertise_addr: SocketAddr,
        grpc_advertise_addr: SocketAddr,
        running_indexing_plan: Option<RunningIndexingPlan>,
    ) -> Self {
        Self {
            node_id,
            start_timestamp,
            enabled_services,
            gossip_advertise_addr,
            grpc_advertise_addr,
            running_indexing_plan,
        }
    }

    pub fn chitchat_id(&self) -> String {
        format!("{}/{}", self.node_id, self.start_timestamp)
    }
}

impl From<ClusterMember> for NodeId {
    fn from(member: ClusterMember) -> Self {
        Self::new(member.chitchat_id(), member.gossip_advertise_addr)
    }
}

/// Running indexing plan on an indexer.
// Note(fmassot) We could have used just a `Vec<IndexingTaks>` but
// we will most likely add some metadata about the running plan in the near future:
// metastore version, errors, applied date, ... thus using a struct is more appropriate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunningIndexingPlan {
    pub indexing_tasks: Vec<IndexingTask>,
}

// Builds cluster members with the given `NodeId`s and `ClusterStateSnapshot`.
pub(crate) fn build_cluster_members(
    node_ids: HashSet<NodeId>,
    cluster_state_snapshot: &ClusterStateSnapshot,
) -> Vec<ClusterMember> {
    node_ids
        .iter()
        .map(|node_id| {
            if let Some(node_state) = cluster_state_snapshot.node_states.get(&node_id.id) {
                build_cluster_member(node_id, node_state)
            } else {
                anyhow::bail!("Could not find node id `{}` in ChitChat state.", node_id.id,)
            }
        })
        .filter_map(|member_res| {
            // Just log an error for members that cannot be built.
            if let Err(error) = &member_res {
                error!(
                    error=?error,
                    "Failed to build cluster member from cluster state, ignoring member.",
                );
            }
            member_res.ok()
        })
        .collect_vec()
}

// Builds a cluster member from [`NodeId`] and [`NodeState`].
pub(crate) fn build_cluster_member(
    chitchat_node: &NodeId,
    node_state: &NodeState,
) -> anyhow::Result<ClusterMember> {
    let enabled_services = node_state
        .get(ENABLED_SERVICES_KEY)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Could not find `{}` key in node `{}` state.",
                ENABLED_SERVICES_KEY,
                chitchat_node.id
            )
        })
        .map(|enabled_services_str| {
            parse_enabled_services_str(enabled_services_str, &chitchat_node.id)
        })??;
    let grpc_advertise_addr = node_state
        .get(GRPC_ADVERTISE_ADDR_KEY)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Could not find `{}` key in node `{}` state.",
                GRPC_ADVERTISE_ADDR_KEY,
                chitchat_node.id
            )
        })
        .map(|addr_str| addr_str.parse::<SocketAddr>())??;
    let running_indexing_plan = node_state
        .get(RUNNING_INDEXING_PLAN)
        .map(serde_json::from_str::<RunningIndexingPlan>)
        .transpose()?;
    let (node_id, start_timestamp_str) = chitchat_node.id.split_once('/').ok_or_else(|| {
        anyhow::anyhow!(
            "Failed to create cluster member instance from NodeId `{}`.",
            chitchat_node.id
        )
    })?;
    let start_timestamp = start_timestamp_str.parse()?;
    Ok(ClusterMember::new(
        node_id.to_string(),
        start_timestamp,
        enabled_services,
        chitchat_node.gossip_public_address,
        grpc_advertise_addr,
        running_indexing_plan,
    ))
}

fn parse_enabled_services_str(
    enabled_services_str: &str,
    node_id: &str,
) -> anyhow::Result<HashSet<QuickwitService>> {
    let enabled_services: HashSet<QuickwitService> = enabled_services_str
        .split(',')
        .filter(|service_str| !service_str.is_empty())
        .filter_map(|service_str| match service_str.parse() {
            Ok(service) => Some(service),
            Err(_) => {
                warn!(
                    node_id=%node_id,
                    service=%service_str,
                    "Found unknown service enabled on node."
                );
                None
            }
        })
        .collect();
    if enabled_services.is_empty() {
        warn!(
            node_id=%node_id,
            "Node has no enabled services."
        )
    }
    Ok(enabled_services)
}
