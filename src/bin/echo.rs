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
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
}

struct EchoNode {
    node_id: String,
    msg_id: usize,
}

impl Node<Payload> for EchoNode {
    fn init(&mut self, node_id: String, _: std::sync::mpsc::Sender<Message<Payload>>) {
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
            Payload::Init {
                ref node_id,
                node_ids: _,
            } => {
                self.node_id = node_id.clone();
                let reply = input_msg.into_reply(self.get_msg_id(), Payload::InitOk);

                writer.write_message(&reply)?;
            }

            Payload::InitOk => panic!("Unexpected InitOk message"),
            Payload::Echo { ref echo } => {
                let echo_reply = echo.clone();
                let reply =
                    input_msg.into_reply(self.get_msg_id(), Payload::EchoOk { echo: echo_reply });
                writer.write_message(&reply)?;
            }
            Payload::EchoOk { .. } => {}
        };
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let mut node = EchoNode {
        msg_id: 1,
        node_id: "0".to_string(),
    };

    process_loop(&mut node)
}
