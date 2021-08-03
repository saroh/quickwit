use std::collections::HashMap;
use std::iter;
use std::sync::Arc;

use crate::models::Batch;
use crate::models::RawBatch;
use anyhow::bail;
use quickwit_actors::Actor;
use quickwit_actors::Mailbox;
use quickwit_actors::SyncActor;
use quickwit_index_config::IndexConfig;
use quickwit_index_config::ShardId;
use quickwit_index_config::StaticRoutingConfig;
use quickwit_index_config::TenantId;
use quickwit_metastore::Checkpoint;
use tantivy::schema::NamedFieldDocument;
use tantivy::Document;
use tantivy::schema::Schema;

const SINGLE_TENANT: &'static str = "_single_tenant";
pub enum RouteConfig {
    SingleTenant {
        route: Route
    } ,
    Multitenant {
        tenant_field: String,
        routes: HashMap<TenantId, Route>,
    },
}

pub struct Router {
    route_config: RouteConfig,
    index_config: Arc<dyn IndexConfig>,
    schema: Schema,
    mailboxes: Vec<Mailbox<Batch>>,
}

impl Router {
    fn shard_id_for_tenant<'a>(&self, doc: &'a NamedFieldDocument) -> Option<&Route> {
        match &self.route_config {
            RouteConfig::SingleTenant { route } => Some(route),
            RouteConfig::Multitenant {
                tenant_field,
                routes,
            } => {
                let tenant_value = doc.0.get(tenant_field)?.first()?;
                let tenant_id = tenant_value.text()?;
                routes.get(tenant_id)
            }
        }
    }
}

impl Actor for Router {
    type Message = RawBatch;

    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {
        ()
    }
}

pub struct Route {
    shard_ord: usize,
    checkpoint: Checkpoint,
}

fn make_route_config(
    static_routing_config: StaticRoutingConfig,
    per_tenant_checkpoint: &HashMap<TenantId, Checkpoint>,
    shard_ids: Vec<ShardId>,
) -> anyhow::Result<RouteConfig> {
    match &static_routing_config {
        StaticRoutingConfig::SingleTenant { shard_config } => {
            //... no need to define any route. All docs will be routed to the first shard.
            let checkpoint = per_tenant_checkpoint.get(SINGLE_TENANT)
                .cloned()
                .unwrap_or_default();
            Ok(RouteConfig::SingleTenant { route: Route { shard_ord: 0, checkpoint }})
        }
        StaticRoutingConfig::Multitenant {
            tenant_key,
            shards,
            tenants,
        } => {
            let mut intermediary_shard_index: HashMap<ShardId, usize> = Default::default();
            for (shard_ord, shard_id) in shard_ids.into_iter().enumerate() {
                intermediary_shard_index.insert(shard_id, shard_ord);
            }
            let mut routes: HashMap<String, Route> = Default::default();
            for (tenant_id, tenant_config) in tenants {
                let checkpoint = per_tenant_checkpoint.get(tenant_id)
                    .cloned()
                    .unwrap_or_default();
                if let Some(&shard_ord) = intermediary_shard_index.get(&tenant_config.target_shard)
                {
                    let checkpoint = per_tenant_checkpoint.get(tenant_id)
                        .cloned()
                        .unwrap_or_default(); // TODO This does not work for a new tenant? We do not want to restart from scratch.
                    routes.insert(tenant_id.to_string(), Route {
                        shard_ord,
                        checkpoint,
                    });
                } else {
                    bail!("Shard {} does not exist.", &tenant_config.target_shard);
                }
            }
            Ok(RouteConfig::Multitenant {
                routes,
                tenant_field: tenant_key.to_string(),
            })
        }
    }
}

impl Router {
    pub fn new(
        static_routing_config: StaticRoutingConfig,
        index_config: Arc<dyn IndexConfig>,
        per_tenant_checkpoint: &HashMap<TenantId, Checkpoint>,
        indexer_mailboxes: HashMap<ShardId, Mailbox<Batch>>,
    ) -> anyhow::Result<Router> {
        let schema = index_config.schema();
        let mut shard_ids = Vec::new();
        let mut mailboxes = Vec::new();
        for (shard_id, mailbox) in indexer_mailboxes {
            shard_ids.push(shard_id);
            mailboxes.push(mailbox);
        }
        let route_config = make_route_config(static_routing_config, per_tenant_checkpoint, shard_ids)?;
        Ok(Router {
            route_config,
            index_config,
            schema,
            mailboxes,
        })
    }

    fn parse_batch(&self, raw_batch: RawBatch) -> Batch {
        let mut docs = Vec::new();
        for doc_json in raw_batch.docs_json {
            // TODO handle parsing error
            if let Ok(doc) = self.index_config.doc_from_json(&doc_json) {
                docs.push(doc);
            }
        }
        Batch {
            docs,
            checkpoint_update: raw_batch.checkpoint_update,
        }
    }
}

impl SyncActor for Router {
    fn process_message(
        &mut self,
        raw_batch: RawBatch,
        progress: &quickwit_actors::Progress,
    ) -> Result<(), quickwit_actors::MessageProcessError> {
        let mut doc_batches: Vec<Vec<Document>> =
            iter::repeat_with(Vec::new).take(self.mailboxes.len()).collect();
        for doc in raw_batch.docs_json {
            if let Ok(named_doc) = serde_json::from_str(&doc) {
                // TODO handle missing tenant_value in doc
                // TODO handle missing tenant_id in config
                if let Some(route) = self.shard_id_for_tenant(&named_doc) {
                    if !route.checkpoint.is_strictly_before(&raw_batch.checkpoint_update) {
                        // The batch is late compared to the index checkpoint for this tenant.
                        // TODO This assumes that the RawBatches are the cut the same way if there is a restart.
                        continue;
                    }
                    if let Ok(doc) = self.schema.convert_named_doc(named_doc) {
                        doc_batches[route.shard_ord].push(doc);
                    } else {
                        // TODO handle parse error
                    }
                }
            } else {
                // TODO handle parse error.
            }
        }
        for (shard_id, docs)  in doc_batches.into_iter().enumerate() {
            if docs.is_empty() {
                continue;
            }
            let batch = Batch {
                docs,
                checkpoint_update: raw_batch.checkpoint_update.clone(),
            };
            self.mailboxes[shard_id].send_blocking(batch)?;
        }
        Ok(())
    }
}
