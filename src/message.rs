use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::io::{StdoutLock, Write};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<P> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<P>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<P> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: P,
}

pub struct MessageWriter {
    output: StdoutLock<'static>,
}

impl MessageWriter {
    pub fn new() -> Self {
        MessageWriter {
            output: std::io::stdout().lock(),
        }
    }

    pub fn write_message<P>(&mut self, message: &Message<P>) -> anyhow::Result<()>
    where
        P: Serialize,
    {
        serde_json::to_writer(&mut self.output, message).context("serialize message")?;
        self.output
            .write_all(b"\n")
            .context("write trailing newline")?;
        Ok(())
    }
}

impl Default for MessageWriter {
    fn default() -> Self {
        Self::new()
    }
}
