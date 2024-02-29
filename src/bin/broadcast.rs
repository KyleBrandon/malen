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
        topology: HashMap<String, HashSet<String>>,
    },
    TopologyOk,
    GossipSend,
    Gossip {
        messages: HashSet<usize>,
    },
}

struct BroadcastNode {
    msg_id: usize,
    node_id: String,
    messages: HashSet<usize>,
    topology: HashSet<String>,
}

impl Node<Payload> for BroadcastNode {
    fn init(&mut self, node_id: String, msg_channel: std::sync::mpsc::Sender<Message<Payload>>) {
        self.node_id = node_id.clone();

        std::thread::spawn(move || {
            // generate gossip events
            // TODO: handle EOF signal
            loop {
                std::thread::sleep(Duration::from_millis(300));
                let gossip = Message {
                    src: node_id.clone(),
                    dest: node_id.clone(),
                    body: Body {
                        msg_id: None,
                        in_reply_to: None,
                        payload: Payload::GossipSend,
                    },
                };
                if msg_channel.send(gossip).is_err() {
                    return Ok::<_, anyhow::Error>(());
                }
            }
        });
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
                let reply = input_msg.into_reply(self.get_msg_id(), Payload::InitOk);

                writer.write_message(&reply)?;
            }

            Payload::InitOk => panic!("Unexpected InitOk message"),
            Payload::GossipSend => {
                //
            }
            Payload::Gossip { messages } => {
                self.messages.extend(messages);
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
                self.topology = topology
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
    let mut node = BroadcastNode {
        msg_id: 0,
        node_id: "0".to_string(),
        messages: HashSet::new(),
        topology: HashSet::new(),
    };

    process_loop(&mut node)
}
