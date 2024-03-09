use std::{collections::HashMap, sync::mpsc::Sender, time::Duration};

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
        delta: i64,
    },
    AddOk,
    Read,
    ReadOk {
        value: i64,
    },
    GossipSend,
    Gossip {
        inc_messages: HashMap<String, i64>,
        dec_messages: HashMap<String, i64>,
    },
    GossipOk,
}

struct Counter {
    msg_id: usize,
    node_id: String,
    node_ids: Vec<String>,
    inc_values: HashMap<String, i64>,
    dec_values: HashMap<String, i64>,
    tx: Option<Sender<Message<Payload>>>,
}

impl Node<Payload> for Counter {
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
                let node_id = node_id.clone();
                let tx = self.tx.clone().unwrap();
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

            Payload::Add { delta } => {
                if delta >= 0 {
                    let value = self.inc_values.entry(self.node_id.clone()).or_insert(0);
                    *value += delta;
                } else {
                    let value = self.dec_values.entry(self.node_id.clone()).or_insert(0);
                    *value += delta.abs();
                }
                let reply = input_msg.into_reply(self.get_msg_id(), Payload::AddOk);
                writer.write_message(&reply)?;
            }

            Payload::AddOk => panic!("Unexpected AddOk message"),

            Payload::Read => {
                let inc_value: i64 = self.inc_values.values().sum();
                let dec_value: i64 = self.dec_values.values().sum();
                let reply = input_msg.into_reply(
                    self.get_msg_id(),
                    Payload::ReadOk {
                        value: inc_value - dec_value,
                    },
                );
                writer.write_message(&reply)?;
            }

            Payload::ReadOk { value: _ } => panic!("Unexpected ReadOk message"),

            Payload::GossipSend => {
                let neighbors = self.node_ids.clone();
                for dest_id in neighbors {
                    let msg_id = self.get_msg_id().expect("No message id");

                    let inc_messages = self.inc_values.clone();
                    let dec_messages = self.dec_values.clone();

                    // send the gossip message
                    let gossip = Message {
                        src: self.node_id.clone(),
                        dest: dest_id.clone(),
                        body: Body {
                            msg_id: Some(msg_id),
                            in_reply_to: None,
                            payload: Payload::Gossip {
                                inc_messages,
                                dec_messages,
                            },
                        },
                    };

                    writer.write_message(&gossip)?;
                }
            }
            Payload::Gossip {
                ref inc_messages,
                ref dec_messages,
            } => {
                for (key, value) in inc_messages {
                    let entry = self.inc_values.entry(key.clone()).or_insert(0);
                    *entry = core::cmp::max(*entry, *value);
                }
                for (key, value) in dec_messages {
                    let entry = self.dec_values.entry(key.clone()).or_insert(0);
                    *entry = core::cmp::max(*entry, *value);
                }

                let reply = input_msg.into_reply(self.get_msg_id(), Payload::GossipOk);
                writer.write_message(&reply)?;
            }

            Payload::GossipOk => {}
        };
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let file_appender =
        tracing_appender::rolling::daily("/Users/kyle/workspaces/malen/log", "maelstrom.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt().with_writer(non_blocking).init();
    let mut node = Counter {
        msg_id: 0,
        node_id: "0".to_string(),
        node_ids: Vec::new(),
        inc_values: HashMap::new(),
        dec_values: HashMap::new(),
        tx: None,
    };

    process_loop(&mut node)
}
