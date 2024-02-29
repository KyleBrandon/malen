use crate::message::{Message, MessageWriter};

pub trait Node<Payload> {
    fn init(&mut self, node_id: String, msg_channel: std::sync::mpsc::Sender<Message<Payload>>);
    fn get_msg_id(&mut self) -> Option<usize>;
    fn handle(
        &mut self,
        input_msg: Message<Payload>,
        writer: &mut MessageWriter,
    ) -> anyhow::Result<()>;
}
