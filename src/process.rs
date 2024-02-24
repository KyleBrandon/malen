use std::io::BufRead;

use crate::{
    message::{Message, MessageWriter},
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

impl InitNode {
    fn new() -> Self {
        InitNode {
            node_id: "0".to_string(),
        }
    }
}

impl Node<Payload> for InitNode {
    fn init(&mut self, _node_id: String) {}

    fn get_msg_id(&mut self) -> Option<usize> {
        None
    }

    fn handle(
        &mut self,
        input_msg: Message<Payload>,
        writer: &mut MessageWriter,
    ) -> anyhow::Result<()> {
        match &input_msg.body.payload {
            Payload::Init {
                node_id,
                node_ids: _,
            } => {
                self.node_id = node_id.clone();
                let reply = input_msg.into_reply(self.get_msg_id(), Payload::InitOk);

                writer.write_message(&reply)?;
            }

            Payload::InitOk => panic!("Unexpected InitOk message"),
        };
        Ok(())
    }
}

struct MessageReader {
    lines: std::io::Lines<std::io::StdinLock<'static>>,
}

impl MessageReader {
    fn new() -> Self {
        let stdin = std::io::stdin().lock();
        let lines = stdin.lines();
        MessageReader { lines }
    }

    fn line(&mut self) -> anyhow::Result<String> {
        self.lines
            .next()
            .expect("Failed to read message")
            .context("failed to read line from stdin")
    }

    fn lines(&mut self) -> &mut std::io::Lines<std::io::StdinLock<'static>> {
        &mut self.lines
    }

    pub fn read_message<P>(&mut self) -> anyhow::Result<Message<P>>
    where
        P: DeserializeOwned,
    {
        serde_json::from_str(&self.line()?).context("failed to read init message")
    }

    // pub fn read_messages<P>(&mut self) -> &impl Iterator<Item = anyhow::Result<Message<P>>>
    // where
    //     P: DeserializeOwned,
    // {
    //     &self.lines.map(|line| -> Result<Message<P>, anyhow::Error> {
    //         //
    //         let line = line.context("failed to read line from stdin")?;
    //         let msg: Message<P> =
    //             serde_json::from_str(&line).context("failed to deserialize message")?;
    //         anyhow::Result::Ok(msg)
    //     })
    // }
}

pub fn process_loop<N, P>(node: &mut N) -> anyhow::Result<()>
where
    N: Node<P>,
    P: DeserializeOwned,
{
    let mut writer = MessageWriter::new();
    let mut reader = MessageReader::new();

    // read the init message
    let init_msg = reader.read_message()?;
    let mut init_node = InitNode::new();
    init_node
        .handle(init_msg, &mut writer)
        .context("InitNode handle function failed")?;

    // initialize the Node with the init message
    node.init(init_node.node_id);

    for line in reader.lines() {
        let line = line.context("Maelstrom input from STDIN could not be read")?;

        let input: Message<P> = serde_json::from_str(&line)
            .context("Maelstrom input from STDIN could not be deserialized")?;
        node.handle(input, &mut writer)
            .context("Node handle function failed")?;
    }

    Ok(())
}
