use crate::database::types::*;

const DB_SIG: &'static str = "BuildYourOwnDB06";
const METAPAGE_SIZE: usize = 32; // 32 Bytes

// | sig | root_ptr | page_used |
// | 16B |    8B    |     8B    |

struct MetaPage(Box<[u8; METAPAGE_SIZE]>);

impl MetaPage {}
