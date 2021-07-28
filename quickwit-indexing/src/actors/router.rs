use std::collections::HashMap;
use std::iter;
use std::sync::Arc;

use anyhow::Context;
use quickwit_actors::Actor;
use quickwit_actors::Mailbox;
use quickwit_actors::SyncActor;
use quickwit_index_config::IndexConfig;
use quickwit_index_config::StaticRoutingConfig;
use quickwit_metastore::Checkpoint;
use tantivy::Document;
use tantivy::schema::Field;
use crate::models::Batch;
use crate::models::RawBatch;

pub struct MailboxWithOffsets {
    pub mailbox: Mailbox<Batch>,
    pub checkpoint: Checkpoint
}

pub struct RouteConfig {
    pub routing_key: String,
    pub sinks: Vec<MailboxWithOffsets>,
    pub routes: HashMap<String, usize>,
}

pub struct Router {
    route_config: RouteConfig,
    routing_field: Field,
    index_config: Arc<dyn IndexConfig>,
}

impl Router {
    fn routing_value<'a>(&self, doc: &'a Document) -> Option<&'a str> {
        let value = doc.get_first(self.routing_field)?;
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


fn make_route_config(static_routing_config: StaticRoutingConfig, sinks: Vec<MailboxWithOffsets>) -> RouteConfig {
    let mut routes: HashMap<String, usize> =
        static_routing_config
            .shards()
            .iter()
            .enumerate()
            .flat_map(|(indexer_ord, shard_config)| {
                shard_config.routing_keys.iter()
                    .map(move |routing_value| {
                        (routing_value.to_string(), indexer_ord)
                    })
            })
            .collect();
    RouteConfig {
        routing_key: static_routing_config.routing_key().to_string(),
        sinks,
        routes,
    }
}


impl Router {
    pub fn new(static_routing_config: StaticRoutingConfig,
               index_config: Arc<dyn IndexConfig>,
               indexer_mailboxes: Vec<MailboxWithOffsets>) -> anyhow::Result<Router> {
        let schema = index_config.schema();
        let routing_field = schema.get_field(static_routing_config.routing_key())
            .with_context(|| format!("Routing field `{}` is not in the schema.", static_routing_config.routing_key()))?;
        let route_config = make_route_config(static_routing_config, indexer_mailboxes);
        Ok(Router {
            route_config,
            index_config,
            routing_field,
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
            .take(self.route_config.sinks.len())
            .collect();
        for doc in batch.docs {
            // TODO handle missing routing value.
            let routing_value = if let Some(routing_value) = self.routing_value(&doc) { routing_value } else { continue; };
            if let Some(&sink_id) = self.route_config.routes.get(routing_value) {
                batches[sink_id].push(doc);
            }
        }
        Ok(())
    }
}
