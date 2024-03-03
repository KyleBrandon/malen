use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use malen::{
    message::{Body, Message, MessageWriter},
    node::Node,
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
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    GossipSend,
    Gossip {
        messages: HashSet<usize>,
    },
    GossipOk,
}

struct BroadcastNode {
    msg_id: usize,
    node_id: String,
    messages: HashSet<usize>,
    neighbors: Vec<String>,
    verified: HashMap<String, HashSet<usize>>,
    gossips_sent: HashMap<usize, HashSet<usize>>,
    last_gossips_sent: HashSet<usize>,
    tx: Option<std::sync::mpsc::Sender<Message<Payload>>>,
}

impl Node<Payload> for BroadcastNode {
    fn init(&mut self, tx: std::sync::mpsc::Sender<Message<Payload>>) {
        self.tx = Some(tx);
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
                node_ids: _,
            } => {
                self.node_id = node_id.clone();

                let node_id = self.node_id.clone();
                let tx = self.tx.clone().unwrap();
                std::thread::spawn(move || loop {
                    std::thread::sleep(Duration::from_millis(350));
                    let gossip = Message {
                        src: node_id.clone(),
                        dest: node_id.clone(),
                        body: Body {
                            msg_id: None,
                            in_reply_to: None,
                            payload: Payload::GossipSend,
                        },
                    };
                    if tx.send(gossip).is_err() {
                        return Ok::<_, anyhow::Error>(());
                    }
                });
                let reply = input_msg.into_reply(self.get_msg_id(), Payload::InitOk);

                writer.write_message(&reply)?;
            }

            Payload::InitOk => panic!("Unexpected InitOk message"),
            Payload::GossipSend => {
                let neighbors: Vec<String> = self.neighbors.clone();
                for dest_id in neighbors {
                    // check for stale gossip messages
                    let current_gossip_msg_ids = self
                        .gossips_sent
                        .keys()
                        .cloned()
                        .collect::<HashSet<usize>>();
                    self.last_gossips_sent
                        .intersection(&current_gossip_msg_ids)
                        .for_each(|msg_id| {
                            self.gossips_sent.remove(msg_id);
                        });

                    // get the messages that we have that we know the dest does not have
                    let gossip_messages: HashSet<usize> = self
                        .messages
                        .difference(self.verified.get(&dest_id).unwrap_or(&HashSet::new()))
                        .cloned()
                        .collect();
                    if gossip_messages.is_empty() {
                        continue;
                    }

                    let msg_id = self.get_msg_id().expect("No message id");

                    // remember what we sent
                    self.gossips_sent
                        .entry(msg_id)
                        .or_default()
                        .extend(gossip_messages.clone());

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
            Payload::Gossip { ref messages } => {
                // add the gossip messages to our set
                self.messages.extend(messages);

                // we know that the source has these messages as well so we don't need to send them
                self.verified
                    .entry(input_msg.src.clone())
                    .or_default()
                    .extend(messages);
                let reply = input_msg.into_reply(self.get_msg_id(), Payload::GossipOk);
                writer.write_message(&reply)?;
            }

            Payload::GossipOk => {
                if let Some(msg_id) = input_msg.body.in_reply_to {
                    // verify what we sent
                    if let Some((_, messages)) = self.gossips_sent.remove_entry(&msg_id) {
                        // we know that the source has received the messages we gossiped
                        self.verified
                            .entry(input_msg.src)
                            .or_default()
                            .extend(messages);
                    }
                }
            }
            Payload::Broadcast { message } => {
                self.messages.insert(message);
                let reply = input_msg.into_reply(self.get_msg_id(), Payload::BroadcastOk);
                writer.write_message(&reply)?;
            }
            Payload::BroadcastOk { .. } => {}
            Payload::Read => {
                let reply = input_msg.into_reply(
                    self.get_msg_id(),
                    Payload::ReadOk {
                        messages: self.messages.clone(),
                    },
                );
                writer.write_message(&reply)?;
            }
            Payload::ReadOk { .. } => {}
            Payload::Topology { ref topology } => {
                self.neighbors = topology
                    .clone()
                    .remove(&self.node_id)
                    .expect("No topology for node");
                let reply = input_msg.into_reply(self.get_msg_id(), Payload::TopologyOk);
                writer.write_message(&reply)?;
            }
            Payload::TopologyOk { .. } => {}
        };
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let file_appender =
        tracing_appender::rolling::daily("/Users/kyle/workspaces/malen/log", "maelstrom.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt().with_writer(non_blocking).init();
    let mut node = BroadcastNode {
        msg_id: 0,
        node_id: "0".to_string(),
        messages: HashSet::new(),
        neighbors: Vec::new(),
        verified: HashMap::new(),
        gossips_sent: HashMap::new(),
        last_gossips_sent: HashSet::new(),
        tx: None,
    };

    process_loop(&mut node)
}
