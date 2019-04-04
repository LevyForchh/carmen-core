use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, PartialOrd, Ord, PartialEq, Eq, Clone)]
pub struct GridKey {
    pub phrase_id: u32,
    pub lang_set: u128
}

#[derive(Serialize, Deserialize, Debug, PartialOrd, Ord, PartialEq, Eq)]
pub struct GridEntry {
    // these will be truncated to 4 bits apiece
    pub relev: u8,
    pub score: u8,
    pub x: u16,
    pub y: u16,
    // this will be truncated to 24 bits
    pub id: u32,
    pub source_phrase_hash: u8
}