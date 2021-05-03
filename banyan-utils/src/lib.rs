#![allow(clippy::upper_case_acronyms)]
pub mod dump;
pub mod ipfs;
pub mod sqlite;
pub mod tag_index;
pub mod tags;

pub fn create_chacha_key(text: String) -> chacha20::Key {
    let mut key = [0u8; 32];
    for (i, v) in text.as_bytes().iter().take(32).enumerate() {
        key[i] = *v;
    }
    key.into()
}
