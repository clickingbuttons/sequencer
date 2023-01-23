use crossbeam_channel::unbounded;
use rand::Rng;
use std::cmp;
use std::collections::HashMap;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const BUFFER_LEN: usize = 10_000;

#[derive(Clone, Hash, Eq, PartialEq, Ord)]
struct BlockHeader {
    seqnum: u64,
    n_messages: u16,
}

impl PartialOrd for BlockHeader {
    fn partial_cmp(&self, r: &Self) -> Option<cmp::Ordering> {
        self.seqnum.partial_cmp(&r.seqnum)
    }
}

#[derive(Clone, Eq, Ord)]
struct BlockMeta {
    seqnum: u64,
    ts: Instant,
}

impl PartialEq for BlockMeta {
    fn eq(&self, r: &Self) -> bool {
        self.seqnum == r.seqnum
    }
}

impl PartialOrd for BlockMeta {
    fn partial_cmp(&self, r: &Self) -> Option<cmp::Ordering> {
        self.seqnum.partial_cmp(&r.seqnum)
    }
}

impl Hash for BlockMeta {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.seqnum.hash(state);
    }
}

fn consume(sink: &mut File, block: BlockHeader) {
    write!(sink, "{}\n", block.seqnum).unwrap();
}

fn generate_blocks(n_blocks: usize) -> Vec<BlockHeader> {
    let mut res = Vec::new();

    // let mut rng = rand::thread_rng();
    let mut seqnum = 0;
    for _i in 0..n_blocks {
        // let n_messages = rng.gen_range(1..50);
        let n_messages = 1; // for easy debugging
        res.push(BlockHeader { seqnum, n_messages });
        seqnum += n_messages as u64
    }
    return res;
}

fn shuffle_blocks(blocks: &Vec<BlockHeader>, thread_num: usize) -> Vec<BlockHeader> {
    // Shuffle 1/100 of the messages to simulate UDP
    let mut res = blocks.clone();
    for i in 1..blocks.len() / 100 {
        let index = i * 4 + thread_num;
        res.swap(index, index + 1);
    }

    return res;
}

fn main() {
    let n_sides = 2;
    let cur_block = Arc::new(Mutex::new(BlockMeta {
        seqnum: 0,
        ts: Instant::now(),
    }));
    let new_blocks = Arc::new(Mutex::new(
        HashMap::<BlockMeta, BlockHeader>::with_capacity(BUFFER_LEN),
    ));
    let (message_sender, message_receiver) = unbounded::<BlockHeader>();

    // Generate some dummy test messages
    let n_blocks = 10_000;
    let mut blocks = generate_blocks(n_blocks);
    // To test buffering delete message 2
    blocks.remove(2);
    // To test multiple timeouts swap 3 and 4
    blocks.swap(2, 3);

    // Start consumer thread
    let r2 = message_receiver.clone();
    let n_consumed = Arc::new(Mutex::new(0 as u64));
    let n_consumed1 = Arc::clone(&n_consumed);
    let consumer = thread::spawn(move || {
        let mut n_consumed1 = n_consumed1.lock().unwrap();
        let mut sink = File::create("messages.txt").unwrap();
        // Sequence up to n messages
        for block in r2.iter() {
            consume(&mut sink, block);
            *n_consumed1 += 1;
        }
    });

    // Start producer threads
    let timeout = Duration::from_millis(10);
    let mut threads = Vec::new();
    for i in 0..n_sides {
        let name = format!("feed {}", i);
        let builder = thread::Builder::new().name(name.clone());

        let cur_block = Arc::clone(&cur_block);
        let blocks = shuffle_blocks(&blocks, i); // Shuffle messages to simulate UDP's Out Of Order
        let new_blocks = Arc::clone(&new_blocks);
        let s = message_sender.clone();
        let thread = builder
            .spawn(move || {
                // TODO: poll network with timeout. On timeout Flush timed out sequence numbers from new_blocks
                for b in blocks {
                    // Receive udp packet which has a block of messages
                    {
                        let mut cur_block = cur_block.lock().unwrap();
                        cur_block.ts = Instant::now();
                        let mut new_blocks = new_blocks.lock().unwrap();
                        if b.seqnum == cur_block.seqnum {
                            cur_block.seqnum += b.n_messages as u64;
                            s.send(b).unwrap();
                        } else if b.seqnum > cur_block.seqnum {
                            let meta = BlockMeta {
                                seqnum: b.seqnum,
                                ts: cur_block.ts,
                            };
                            if new_blocks.insert(meta.clone(), b).is_none() {
                                println!("Out of order {} (expected {})", meta.seqnum, cur_block.seqnum);
                            }
                        }
                        // Flush in order sequence numbers from new_blocks
                        while let Some(new_block) = new_blocks.remove(&cur_block) {
                            cur_block.seqnum += new_block.n_messages as u64;
                            s.send(new_block).unwrap();
                            cur_block.ts = Instant::now();
                        }
                        // Flush timed out sequence numbers from new_blocks
                        if new_blocks.len() > 0 {
                            let mut block_metas = Vec::<BlockMeta>::new();
                            // Pass 1: Collect timed out blocks. Find minimum seqnum
                            let mut min_seqnum = std::u64::MAX;
                            for meta in new_blocks.keys() {
                                let duration = cur_block.ts.duration_since(meta.ts);
                                if duration > timeout {
                                    println!("Timeout {} (duration {:?} > {:?})", meta.seqnum, duration, timeout);
                                    block_metas.push(meta.clone());
                                    if meta.seqnum < min_seqnum {
                                        min_seqnum = meta.seqnum
                                    }
                                }
                            }

                            if block_metas.len() > 0 {
                                // Pass 2: Collect seqnums earlier than minimum seqnum
                                for meta in new_blocks.keys() {
                                    if meta.seqnum < min_seqnum {
                                        block_metas.push(meta.clone());
                                    }
                                }
                                block_metas.sort_by_key(|b| b.seqnum);

                                println!(
                                    "Flushing {} blocks from {} to {}",
                                    block_metas.len(),
                                    block_metas[0].seqnum,
                                    block_metas[block_metas.len() - 1].seqnum
                                );
                                for m in block_metas {
                                    let b = new_blocks.remove(&m).unwrap();
                                    cur_block.seqnum = b.seqnum + b.n_messages as u64;
                                    s.send(b).unwrap();
                                }
                            }
                        }
                    }
                    // Simulate time between packets
                    let n = rand::thread_rng().gen_range(0..50);
                    thread::sleep(std::time::Duration::from_micros(n));
                }
            })
            .unwrap();
        threads.push(thread);
    }

    // Wait for them all to stop
    for t in threads {
        t.join().unwrap();
    }
    drop(message_sender); // To end consumer thread's iter
    consumer.join().unwrap();

    println!(
        "Consumed {} blocks, ending seqnum {}",
        *n_consumed.clone().lock().unwrap(),
        cur_block.clone().lock().unwrap().seqnum
    );
}
