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
    Generate,
    GenerateOk { id: String },
}

struct GenerateNode {
    node_id: String,
    msg_id: usize,
}

impl Node<Payload> for GenerateNode {
    fn init(&mut self, node_id: String) {
        self.node_id = node_id;
    }

    fn handle(
        &mut self,
        input_msg: &Message<Payload>,
        writer: &mut MessageWriter,
    ) -> anyhow::Result<()> {
        match &input_msg.body.payload {
            Payload::Generate => {
                let reply = Message {
                    src: self.node_id.clone(),
                    dst: input_msg.src.clone(),
                    body: Body {
                        msg_id: Some(self.msg_id),
                        in_reply_to: input_msg.body.msg_id,
                        payload: Payload::GenerateOk {
                            id: format!("{}-{}", self.node_id, self.msg_id),
                        },
                    },
                };
                writer.write_message(&reply)?;
                self.msg_id += 1;
            }
            Payload::GenerateOk { .. } => {}
        };
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let mut node = GenerateNode {
        msg_id: 1,
        node_id: "0".to_string(),
    };

    process_loop(&mut node)
}
