use std::ops::Deref;
use std::ops::DerefMut;

use crate::database::{helper::*, types::*};

const DB_SIG: &'static str = "BuildYourOwnDB06";
const METAPAGE_SIZE: usize = 32; // 32 Bytes
const SIG_SIZE: usize = 16; // 32 Bytes
const ROOTPTR_SIZE: usize = 8; // 32 Bytes
const NPAGES_SIZE: usize = 8; // 32 Bytes

// | sig | root_ptr | page_used |
// | 16B |    8B    |     8B    |

struct MetaPage(Box<[u8; METAPAGE_SIZE]>);

impl MetaPage {
    fn new() -> Self {
        MetaPage(Box::new([0; METAPAGE_SIZE]))
    }
    fn set_sig(&mut self) {
        self[..SIG_SIZE].copy_from_slice(DB_SIG.as_bytes());
    }
    fn set_root(&mut self, ptr: Pointer) {
        write_pointer(self, SIG_SIZE, ptr);
    }
}

impl Deref for MetaPage {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0[..]
    }
}

impl DerefMut for MetaPage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0[..]
    }
}
