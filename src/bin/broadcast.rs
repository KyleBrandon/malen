use std::collections::HashMap;

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
    Broadcast {
        message: u32,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: Vec<u32>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

struct BroadcastNode {
    msg_id: usize,
    node_id: String,
    messages: Vec<u32>,
}

impl Node<Payload> for BroadcastNode {
    fn init(&mut self, node_id: String) {
        self.node_id = node_id;
    }

    fn handle(
        &mut self,
        input_msg: &Message<Payload>,
        writer: &mut MessageWriter,
    ) -> anyhow::Result<()> {
        match &input_msg.body.payload {
            Payload::Broadcast { message } => {
                self.messages.push(*message);
                let reply = Message {
                    src: self.node_id.clone(),
                    dst: input_msg.src.clone(),
                    body: Body {
                        msg_id: Some(self.msg_id),
                        in_reply_to: input_msg.body.msg_id,
                        payload: Payload::BroadcastOk,
                    },
                };
                self.msg_id += 1;
                writer.write_message(&reply)?;
            }
            Payload::BroadcastOk { .. } => {}
            Payload::Read => {
                let reply = Message {
                    src: self.node_id.clone(),
                    dst: input_msg.src.clone(),
                    body: Body {
                        msg_id: Some(self.msg_id),
                        in_reply_to: input_msg.body.msg_id,
                        payload: Payload::ReadOk {
                            messages: self.messages.clone(),
                        },
                    },
                };
                self.msg_id += 1;
                writer.write_message(&reply)?;
            }
            Payload::ReadOk { .. } => {}
            Payload::Topology { topology: _ } => {
                let reply = Message {
                    src: self.node_id.clone(),
                    dst: input_msg.src.clone(),
                    body: Body {
                        msg_id: Some(self.msg_id),
                        in_reply_to: input_msg.body.msg_id,
                        payload: Payload::TopologyOk,
                    },
                };
                self.msg_id += 1;
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
        messages: Vec::new(),
    };

    process_loop(&mut node)
}
