use std::collections::{HashMap, HashSet};

use malen::{
    message::{Message, MessageWriter},
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
        messages: HashSet<u32>,
    },
    Topology {
        topology: HashMap<String, HashSet<String>>,
    },
    TopologyOk,
}

struct BroadcastNode {
    msg_id: usize,
    node_id: String,
    messages: HashSet<u32>,
    topology: HashMap<String, HashSet<String>>,
}

impl Node<Payload> for BroadcastNode {
    fn init(&mut self, node_id: String) {
        self.node_id = node_id;
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
                self.topology = topology.clone();
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
        topology: HashMap::new(),
    };

    process_loop(&mut node)
}
