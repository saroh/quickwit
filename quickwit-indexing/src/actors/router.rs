use std::collections::HashMap;
use std::iter;
use std::sync::Arc;

use anyhow::Context;
use anyhow::bail;
use quickwit_actors::Actor;
use quickwit_actors::Mailbox;
use quickwit_actors::SyncActor;
use quickwit_index_config::IndexConfig;
use quickwit_index_config::ShardId;
use quickwit_index_config::StaticRoutingConfig;
use tantivy::Document;
use tantivy::schema::Field;
use crate::models::Batch;
use crate::models::RawBatch;

pub struct RouteConfig {
    pub tenant_key: String,
    pub mailboxes: Vec<Mailbox<Batch>>,
    pub routes: HashMap<String, usize>,
}

pub struct Router {
    route_config: RouteConfig,
    tenant_field: Field,
    index_config: Arc<dyn IndexConfig>,
}

impl Router {
    fn tenant_id<'a>(&self, doc: &'a Document) -> Option<&'a str> {
        let value = doc.get_first(self.tenant_field)?;
        value.text()
   }
}

impl Actor for Router {

    type Message = RawBatch;

    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {
        ()
    }
}


fn make_route_config(static_routing_config: StaticRoutingConfig, sinks: HashMap<ShardId, Mailbox<Batch>>) -> anyhow::Result<RouteConfig> {
    let mut mailboxes = Vec::new();
    let mut intermediary_shard_index: HashMap<ShardId, usize> = Default::default();
    for (shard_ord, (shard_id, sink)) in sinks.into_iter().enumerate() {
        intermediary_shard_index.insert(shard_id, shard_ord);
        mailboxes.push(sink);
    }
    let mut routes: HashMap<String, usize> = Default::default();
    for (tenant_id, tenant_config) in static_routing_config.tenants() {
        if let Some(&shard_ord) = intermediary_shard_index.get(&tenant_config.target_shard) {
            routes.insert(tenant_id.to_string(), shard_ord);
        } else {
            bail!("Shard {} does not exist.", &tenant_config.target_shard);
        }
    }
    Ok(RouteConfig {
        tenant_key: static_routing_config.tenant_key().to_string(),
        mailboxes,
        routes,
    })
}


impl Router {
    pub fn new(static_routing_config: StaticRoutingConfig,
               index_config: Arc<dyn IndexConfig>,
               indexer_mailboxes: HashMap<ShardId, Mailbox<Batch>>) -> anyhow::Result<Router> {
        let schema = index_config.schema();
        let tenant_field = schema.get_field(static_routing_config.tenant_key())
            .with_context(|| format!("Routing field `{}` is not in the schema.", static_routing_config.tenant_key()))?;
        let route_config = make_route_config(static_routing_config, indexer_mailboxes)?;
        Ok(Router {
            route_config,
            index_config,
            tenant_field,
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
        let batch = self.parse_batch(raw_batch);
        let mut batches: Vec<Vec<Document>> = iter::repeat_with(Vec::new)
            .take(self.route_config.mailboxes.len())
            .collect();
        for doc in batch.docs {
            // TODO handle missing routing value.
            let routing_value = if let Some(routing_value) = self.tenant_id(&doc) { routing_value } else { continue; };
            if let Some(&sink_id) = self.route_config.routes.get(routing_value) {
                batches[sink_id].push(doc);
            }
        }
        Ok(())
    }
}
