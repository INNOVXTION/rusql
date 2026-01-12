use tracing::debug;

use crate::database::{btree::Compare, tables::Key};

/// struct to record modified keys
///
/// works like a music recorder, you set it to .listen() and it will capture every key  touched by a tree_set() or tree_delete() operation. By calling .capture() it will record a snapshot with a range or a single key. This is useful for range operations like deleting a whole table.
pub struct KeyRange {
    pub recorded: Vec<Touched>,
    listen: bool,

    first: Option<Key>,
    last: Option<Key>,
}

pub enum Touched {
    Single(Key),
    Range { from: Key, to: Key },
}

impl std::fmt::Display for Touched {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Touched::Single(key) => write!(f, "Single: {key}"),
            Touched::Range { from, to } => write!(f, "Range, from: {from} to: {to}"),
        }
    }
}

impl Touched {
    fn as_range(&self) -> (&Key, &Key) {
        match self {
            Touched::Single(k) => (k, k),
            Touched::Range { from, to } => (from, to),
        }
    }

    pub fn conflict(tx: &[Self], history: &[Self]) -> bool {
        for a in tx {
            let (a_lo, a_hi) = a.as_range();

            for b in history {
                let (b_lo, b_hi) = b.as_range();

                if a_lo <= b_hi && a_hi >= b_lo {
                    return true;
                }
            }
        }
        false
    }
}

impl KeyRange {
    pub fn new() -> Self {
        KeyRange {
            recorded: vec![],
            listen: false,
            first: None,
            last: None,
        }
    }

    /// starts to keep track of touched keys, discards previously listened to keys when called
    pub fn listen(&mut self) {
        if self.listen {
            self.first = None;
            self.last = None;
        } else {
            self.listen = true;
        }
    }

    pub fn add(&mut self, key: &Key) {
        debug!(key=%key, "adding:");
        if self.listen {
            if self.first.is_none() {
                self.first = Some(key.clone());
            } else {
                self.last = Some(key.clone())
            }
        }
    }

    /// records a snapshot of first and last key touched, stops listening afterwards
    ///
    /// use this when modifiying contiguous ranges of keys
    pub fn capture_and_stop(&mut self) {
        // no capture
        if self.last.is_none() && self.first.is_none() {
            self.listen = false;
            return;
        }

        // faulty capture
        if self.last.is_some() && self.first.is_none() {
            panic!("edge case that shouldnt happen");
        }

        // single key capture
        if self.last.is_none() && self.first.is_some() {
            let key = Touched::Single(self.first.take().unwrap().clone());
            debug!(key=%key, "captured:");

            self.recorded.push(key);
            self.listen = false;
            return;
        }

        // range capture
        let lo = self.first.take().unwrap().clone();
        let hi = self.last.take().unwrap().clone();

        let range = match lo.cmp(&hi) {
            std::cmp::Ordering::Less => Touched::Range { from: lo, to: hi },
            std::cmp::Ordering::Greater => Touched::Range { from: hi, to: lo },
            std::cmp::Ordering::Equal => Touched::Single(lo),
        };

        debug!(key=%range, "captured:");
        self.recorded.push(range);
        self.listen = false;
    }

    /// records currently captured keys and starts a new liste
    ///
    /// useful for repeated single modifications
    pub fn capture_and_listen(&mut self) {
        // no capture
        if self.last.is_none() && self.first.is_none() {
            return;
        }

        // faulty capture
        if self.last.is_some() && self.first.is_none() {
            panic!("edge case that shouldnt happen");
        }

        // single key capture
        if self.last.is_none() && self.first.is_some() {
            let key = Touched::Single(self.first.take().unwrap().clone());
            debug!(key=%key, "captured:");

            self.recorded.push(key);
            self.first = None;
            self.last = None;
            return;
        }

        // range capture
        let lo = self.first.take().unwrap().clone();
        let hi = self.last.take().unwrap().clone();

        let range = match lo.cmp(&hi) {
            std::cmp::Ordering::Less => Touched::Range { from: lo, to: hi },
            std::cmp::Ordering::Greater => Touched::Range { from: hi, to: lo },
            std::cmp::Ordering::Equal => Touched::Single(lo),
        };
        debug!(key=%range, "captured:");
        self.recorded.push(range);
    }

    pub fn print(&self) {
        debug!(len = self.recorded.len(), "recorded keys:");
        for e in self.recorded.iter() {
            debug!(touched=%e);
        }
    }
}
