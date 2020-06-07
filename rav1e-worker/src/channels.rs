use crate::worker::SlotQueueItem;
use crossbeam_channel::{Receiver, Sender};

pub type SlotRequestSender = Sender<SlotQueueItem>;
pub type SlotRequestReceiver = Receiver<SlotQueueItem>;
pub type SlotRequestChannel = (SlotRequestSender, SlotRequestReceiver);
