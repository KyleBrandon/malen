use crate::{
    message::{Message, MessageReader, MessageWriter},
    node::Node,
};
use anyhow::Context;
use serde::de::DeserializeOwned;

pub fn process_loop<N, P>(node: &mut N) -> anyhow::Result<()>
where
    N: Node<P> + Send,
    P: DeserializeOwned + Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        let mut reader = MessageReader::new();

        for line in reader.lines() {
            let line = line.context("Maelstrom input from STDIN could not be read")?;

            let input: Message<P> = serde_json::from_str(&line)
                .context("Maelstrom input from STDIN could not be deserialized")?;

            // TODO: handle EOF signal
            if tx.send(input).is_err() {
                return Ok::<_, anyhow::Error>(());
            }
        }
        // TODO: send EOF signal
        //
        Ok(())
    });

    let mut writer = MessageWriter::new();

    for input in rx {
        node.handle(input, &mut writer)
            .context("Node handle function failed")?;
    }

    thread
        .join()
        .expect("Failed to join thread")
        .context("Failed to join thread")?;

    Ok(())
}
