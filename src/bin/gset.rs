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
    verified: HashMap<String, HashSet<usize>>,
    gossips_sent: HashMap<usize, HashSet<usize>>,
    last_gossips_sent: HashSet<usize>,
    tx: Option<std::sync::mpsc::Sender<Message<Payload>>>,
}

impl Node<Payload> for GSetNode {
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
                ref node_ids,
            } => {
                self.node_id = node_id.clone();
                self.node_ids = node_ids.clone();

                let tx = self.tx.clone().unwrap();
                let node_id = self.node_id.clone();
                std::thread::spawn(move || loop {
                    std::thread::sleep(Duration::from_millis(5000));
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
                    // check for stale gossip messages
                    let current_gossip_msg_ids = self
                        .gossips_sent
                        .keys()
                        .cloned()
                        .collect::<HashSet<usize>>();
                    self.last_gossips_sent
                        .intersection(&current_gossip_msg_ids)
                        .for_each(|msg_id| {
                            tracing::info!("Removing stale gossip message: {}", &msg_id);
                            self.gossips_sent.remove(msg_id);
                        });

                    // get the messages that we have that we know the dest does not have
                    let gossip_messages: HashSet<usize> = self
                        .values
                        .difference(self.verified.get(&dest_id).unwrap_or(&HashSet::new()))
                        .cloned()
                        .collect();
                    if gossip_messages.is_empty() {
                        tracing::info!("No gossip messages to send to {}", &dest_id);
                        continue;
                    }

                    let msg_id = self.get_msg_id().expect("No message id");

                    // remember what we sent
                    self.gossips_sent
                        .entry(msg_id)
                        .or_default()
                        .extend(gossip_messages.clone());

                    tracing::info!(
                        "Sending gossip messages to {}: {:?}",
                        &dest_id,
                        &gossip_messages
                    );

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
                tracing::info!(
                    "Received gossip messages from {}: {:?}",
                    input_msg.src.clone(),
                    &messages
                );
                // add the gossip messages to our set
                self.values.extend(messages);

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
                    tracing::info!("Received GossipOk from {}", input_msg.src.clone());
                    // verify what we sent
                    if let Some((_, messages)) = self.gossips_sent.remove_entry(&msg_id) {
                        tracing::info!(
                            "gossips_sent removed entry for {}: {:?}",
                            input_msg.src.clone(),
                            &messages
                        );
                        // we know that the source has received the messages we gossiped
                        self.verified
                            .entry(input_msg.src)
                            .or_default()
                            .extend(messages);
                    }
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
        verified: HashMap::new(),
        gossips_sent: HashMap::new(),
        last_gossips_sent: HashSet::new(),
        tx: None,
    };

    process_loop(&mut node)
}
