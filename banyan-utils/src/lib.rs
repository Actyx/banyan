pub mod dump;
pub mod ipfs;
pub mod sqlite;
pub mod tag_index;
pub mod tags;

pub fn create_salsa_key(text: String) -> salsa20::Key {
    let mut key = [0u8; 32];
    for (i, v) in text.as_bytes().iter().take(32).enumerate() {
        key[i] = *v;
    }
    key.into()
}
