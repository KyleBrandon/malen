use std::collections::HashSet;

use malen::{
    message::{Body, Message, MessageWriter},
    node::{GossipManager, Node},
    process::process_loop,
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Add {
        element: usize,
    },
    AddOk,
    Read,
    ReadOk {
        value: HashSet<usize>,
    },
    GossipSend,
    Gossip {
        messages: HashSet<usize>,
    },
    GossipOk,
}

struct GSetNode {
    msg_id: usize,
    node_id: String,
    node_ids: Vec<String>,
    values: HashSet<usize>,
    gossip_manager: Option<GossipManager<Payload, usize>>,
}

impl Node<Payload> for GSetNode {
    fn init(&mut self, tx: std::sync::mpsc::Sender<Message<Payload>>) {
        self.gossip_manager = Some(GossipManager::new(tx.clone()));
    }

    fn get_msg_id(&mut self) -> Option<usize> {
        self.msg_id += 1;

        Some(self.msg_id)
    }

    fn handle(
        &mut self,
        input_msg: Message<Payload>,
        writer: &mut MessageWriter,
    ) -> anyhow::Result<()> {
        match input_msg.body.payload {
            Payload::Init {
                ref node_id,
                ref node_ids,
            } => {
                self.node_id = node_id.clone();
                self.node_ids = node_ids.clone();

                if let Some(manager) = &self.gossip_manager {
                    manager.create_gossip_monitor(self.node_id.clone(), Payload::GossipSend);
                }

                let reply = input_msg.into_reply(self.get_msg_id(), Payload::InitOk);

                writer.write_message(&reply)?;
            }

            Payload::InitOk => panic!("Unexpected InitOk message"),

            Payload::Add { element } => {
                self.values.insert(element);
                let reply = input_msg.into_reply(self.get_msg_id(), Payload::AddOk);
                writer.write_message(&reply)?;
            }

            Payload::AddOk => panic!("Unexpected AddOk message"),

            Payload::Read => {
                let reply = input_msg.into_reply(
                    self.get_msg_id(),
                    Payload::ReadOk {
                        value: self.values.clone(),
                    },
                );
                writer.write_message(&reply)?;
            }

            Payload::ReadOk { value: _ } => panic!("Unexpected ReadOk message"),

            Payload::GossipSend => {
                let neighbors = self.node_ids.clone();
                for dest_id in neighbors {
                    let msg_id = self.get_msg_id().expect("No message id");

                    if let Some(manager) = &mut self.gossip_manager {
                        manager.prune_stale_sent_gossips();
                        let gossip_messages =
                            manager.get_messages_to_send(&dest_id, msg_id, &self.values);
                        if gossip_messages.is_empty() {
                            tracing::info!("No gossip messages to send to {}", &dest_id);
                            continue;
                        }

                        // send the gossip message
                        let gossip = Message {
                            src: self.node_id.clone(),
                            dest: dest_id.clone(),
                            body: Body {
                                msg_id: Some(msg_id),
                                in_reply_to: None,
                                payload: Payload::Gossip {
                                    messages: gossip_messages,
                                },
                            },
                        };

                        writer.write_message(&gossip)?;
                    }
                }
            }
            Payload::Gossip { ref messages } => {
                tracing::info!(
                    "Received gossip messages from {}: {:?}",
                    input_msg.src.clone(),
                    &messages
                );

                // add the gossip messages to our set
                self.values.extend(messages);

                if let Some(manager) = &mut self.gossip_manager {
                    manager.verify_messages(&input_msg.src, messages.clone());

                    let reply = input_msg.into_reply(self.get_msg_id(), Payload::GossipOk);
                    writer.write_message(&reply)?;
                }
            }

            Payload::GossipOk => {
                if let Some(manager) = &mut self.gossip_manager {
                    manager.handle_gossip_ok(input_msg);
                }
            }
        };
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let file_appender =
        tracing_appender::rolling::daily("/Users/kyle/workspaces/malen/log", "maelstrom.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt().with_writer(non_blocking).init();
    let mut node = GSetNode {
        msg_id: 0,
        node_id: "0".to_string(),
        node_ids: Vec::new(),
        values: HashSet::new(),
        gossip_manager: None,
    };

    process_loop(&mut node)
}
