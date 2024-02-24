use std::io::BufRead;

use crate::{
    message::{Body, Message, MessageWriter},
    node::Node,
};
use anyhow::Context;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
}

struct InitNode {
    node_id: String,
}

impl Node<Payload> for InitNode {
    fn init(&mut self, _node_id: String) {}
    fn handle(
        &mut self,
        input_msg: &Message<Payload>,
        writer: &mut MessageWriter,
    ) -> anyhow::Result<()> {
        match &input_msg.body.payload {
            Payload::Init {
                node_id,
                node_ids: _,
            } => {
                self.node_id = node_id.clone();

                let reply = Message {
                    src: self.node_id.clone(),
                    dst: input_msg.src.clone(),
                    body: Body {
                        msg_id: None,
                        in_reply_to: input_msg.body.msg_id,
                        payload: Payload::InitOk {},
                    },
                };
                writer.write_message(&reply)?;
            }

            Payload::InitOk => panic!("Unexpected InitOk message"),
        };
        Ok(())
    }
}

pub fn process_loop<N, P>(node: &mut N) -> anyhow::Result<()>
where
    N: Node<P>,
    P: DeserializeOwned,
{
    let mut writer = MessageWriter::new();

    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();

    let init_msg = serde_json::from_str(
        &stdin
            .next()
            .expect("no init message received")
            .context("failed to read init message from stdin")?,
    )
    .context("init message could not be deserialized")?;

    let mut init_node = InitNode {
        node_id: "0".to_string(),
    };

    init_node
        .handle(&init_msg, &mut writer)
        .context("InitNode handle function failed")?;

    node.init(init_node.node_id);

    for line in stdin {
        let line = line.context("Maelstrom input from STDIN could not be read")?;
        let input: Message<P> = serde_json::from_str(&line)
            .context("Maelstrom input from STDIN could not be deserialized")?;
        node.handle(&input, &mut writer)
            .context("Node handle function failed")?;
    }

    Ok(())
}
