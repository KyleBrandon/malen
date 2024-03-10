use malen::{
    message::{Message, MessageWriter},
    node::Node,
    process::process_loop,
};
use std::{
    collections::{BTreeMap, HashMap},
    ops::Bound::Included,
    sync::mpsc::Sender,
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
    Send {
        key: String,
        msg: u64,
    },
    SendOk {
        offset: u64,
    },
    Poll {
        offsets: HashMap<String, u64>,
    },
    PollOk {
        msgs: HashMap<String, Vec<Vec<u64>>>,
    },
    CommitOffsets {
        offsets: HashMap<String, u64>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, u64>,
    },
}

#[derive(Default)]
struct Log {
    committed_offset: u64,
    current_offset: u64,
    messages: BTreeMap<u64, u64>,
}
impl Log {
    fn insert_message(&mut self, msg: u64) -> u64 {
        self.current_offset += 1;
        self.messages.insert(self.current_offset, msg);

        self.current_offset
    }

    fn poll_commits(&self, offset: &u64) -> Vec<Vec<u64>> {
        let commits: Vec<Vec<u64>> = self
            .messages
            .range((Included(offset), Included(&(offset + 10))))
            .map(|(k, v)| vec![*k, *v])
            .collect();
        commits
    }

    fn commit(&mut self, offset: u64) {
        self.committed_offset = offset;
    }
}

struct Kafka {
    msg_id: usize,
    node_id: String,
    node_ids: Vec<String>,
    logs: HashMap<String, Log>,
    tx: Option<Sender<Message<Payload>>>,
}

impl Node<Payload> for Kafka {
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

                let reply = input_msg.into_reply(self.get_msg_id(), Payload::InitOk);
                writer.write_message(&reply)?;
            }

            Payload::InitOk => panic!("Unexpected InitOk message"),

            Payload::Send { ref key, ref msg } => {
                tracing::info!("Received Send message: key: {}, msg: {}", key, msg);
                let log = self.logs.entry(key.clone()).or_default();
                let offset = log.insert_message(*msg);

                let reply = input_msg.into_reply(self.get_msg_id(), Payload::SendOk { offset });
                writer.write_message(&reply)?;
            }

            Payload::SendOk { offset: _ } => {
                tracing::info!("Received SendOk message");
            }

            Payload::Poll { ref offsets } => {
                tracing::info!("Received Poll message: {:?}", offsets);

                let mut msgs = HashMap::new();
                for (key, offset) in offsets {
                    let log = self.logs.entry(key.clone()).or_default();
                    let commits = log.poll_commits(offset);

                    msgs.insert(key.clone(), commits);
                }

                tracing::info!("Sending PollOk message: {:?}", msgs);

                let reply = input_msg.into_reply(self.get_msg_id(), Payload::PollOk { msgs });
                writer.write_message(&reply)?;
            }

            Payload::PollOk { ref msgs } => {
                tracing::info!("Received PollOk message: {:?}", msgs);
            }
            Payload::CommitOffsets { ref offsets } => {
                tracing::info!("Received CommitOffsets message: {:?}", offsets);
                for (key, offset) in offsets {
                    let log = self.logs.entry(key.clone()).or_default();
                    log.commit(*offset);
                }

                let reply = input_msg.into_reply(self.get_msg_id(), Payload::CommitOffsetsOk);
                writer.write_message(&reply)?;
            }
            Payload::CommitOffsetsOk => {
                tracing::info!("Received CommitOffsetsOk message");
            }
            Payload::ListCommittedOffsets { ref keys } => {
                tracing::info!("Received ListCommittedOffsets message: {:?}", keys);
                let mut committed_offsets = HashMap::new();
                for key in keys {
                    let log = self.logs.entry(key.clone()).or_default();
                    committed_offsets.insert(key.clone(), log.committed_offset);
                }
                let reply = input_msg.into_reply(
                    self.get_msg_id(),
                    Payload::ListCommittedOffsetsOk {
                        offsets: HashMap::new(),
                    },
                );
                writer.write_message(&reply)?;
            }
            Payload::ListCommittedOffsetsOk { ref offsets } => {
                tracing::info!("Received ListCommittedOffsetsOk message: {:?}", offsets);
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
    let mut node = Kafka {
        msg_id: 0,
        node_id: "0".to_string(),
        node_ids: Vec::new(),
        logs: HashMap::new(),
        tx: None,
    };

    process_loop(&mut node)
}
