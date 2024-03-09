use std::{
    collections::{HashMap, HashSet},
    sync::mpsc::Sender,
    time::Duration,
};

use crate::message::{Body, Message, MessageWriter};

pub trait Node<Payload>
where
    Payload: Send + Clone + 'static,
{
    fn init(&mut self, tx: std::sync::mpsc::Sender<Message<Payload>>);
    fn get_msg_id(&mut self) -> Option<usize>;
    fn handle(
        &mut self,
        input_msg: Message<Payload>,
        writer: &mut MessageWriter,
    ) -> anyhow::Result<()>;
}

pub struct GossipManager<Payload, T>
where
    Payload: Send + Clone + 'static,
    T: std::cmp::Eq + std::hash::Hash + Clone,
{
    node_id: String,
    node_ids: Vec<String>,
    gossips_sent: HashMap<usize, HashSet<T>>,
    stale_gossips_sent: HashSet<usize>,
    verified: HashMap<String, Vec<T>>,
    tx: Sender<Message<Payload>>,
}

impl<Payload, T> GossipManager<Payload, T>
where
    Payload: Send + Clone + 'static,
    T: std::cmp::Eq + std::hash::Hash + Clone,
{
    pub fn new(tx: Sender<Message<Payload>>) -> Self {
        Self {
            node_id: "0".to_string(),
            node_ids: Vec::new(),
            gossips_sent: HashMap::new(),
            stale_gossips_sent: HashSet::new(),
            verified: HashMap::new(),
            tx,
        }
    }

    pub fn create_gossip_monitor(
        &mut self,
        node_id: String,
        node_ids: Vec<String>,
        payload: Payload,
    ) {
        self.node_id = node_id.clone();
        self.node_ids = node_ids.clone();
        let tx = self.tx.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(5000));
            let gossip = Message {
                src: node_id.clone(),
                dest: node_id.clone(),
                body: Body {
                    msg_id: None,
                    in_reply_to: None,
                    payload: payload.clone(),
                },
            };
            if tx.send(gossip).is_err() {
                return Ok::<_, anyhow::Error>(());
            }
        });
    }

    pub fn prune_stale_sent_gossips(
        &mut self,
        dest_id: &str,
        msg_id: usize,
        values: &Vec<T>,
    ) -> Vec<T> {
        // check for stale gossip messages
        let current_gossip_msg_ids = self
            .gossips_sent
            .keys()
            .cloned()
            .collect::<HashSet<usize>>();
        // remove any gossips that have not been responded to
        self.stale_gossips_sent
            .intersection(&current_gossip_msg_ids)
            .for_each(|msg_id| {
                tracing::info!("Removing stale gossip message: {}", &msg_id);
                self.gossips_sent.remove(msg_id);
            });

        // update the stale gossips with the current list of outstanding gossips
        self.stale_gossips_sent = self
            .gossips_sent
            .keys()
            .cloned()
            .collect::<HashSet<usize>>();

        // only send messages that we know the destination does not have
        let gossip_messages: Vec<T> = values
            .iter()
            .filter(|v| {
                !self
                    .verified
                    .get(dest_id)
                    .unwrap_or(&Vec::new())
                    .contains(v)
            })
            .cloned()
            .collect();

        // remember what we sent
        self.gossips_sent
            .entry(msg_id)
            .or_default()
            .extend(gossip_messages.clone());

        gossip_messages
    }

    pub fn verify_messages(&mut self, src: &str, messages: Vec<T>) {
        self.verified
            .entry(src.to_string())
            .or_default()
            .extend(messages);
    }

    pub fn handle_gossip_ok(&mut self, input_msg: Message<Payload>) {
        if let Some(msg_id) = input_msg.body.in_reply_to {
            tracing::info!("Received GossipOk from {}", input_msg.src.clone());
            // verify what we sent
            if let Some((_, messages)) = self.gossips_sent.remove_entry(&msg_id) {
                tracing::info!("gossips_sent removed entry for {}", input_msg.src.clone(),);
                // we know that the source has received the messages we gossiped
                self.verified
                    .entry(input_msg.src)
                    .or_default()
                    .extend(messages);
            }
        }
    }
}
