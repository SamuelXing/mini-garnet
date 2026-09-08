//! 64-bit key hashing. Tsavorite derives the bucket index from the high bits
//! of the hash and the tag from the next bits (see `index.rs`), so the hash
//! must mix well in the top bits. This is a compact wyhash-style mixer.

#[inline]
fn mum(a: u64, b: u64) -> u64 {
    let r = (a as u128).wrapping_mul(b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

const P0: u64 = 0xa076_1d64_78bd_642f;
const P1: u64 = 0xe703_7ed1_a0b4_28db;
const P2: u64 = 0x8ebc_6af0_9c88_c6e3;

#[inline]
fn read_u64(b: &[u8]) -> u64 {
    u64::from_le_bytes(b[..8].try_into().unwrap())
}
#[inline]
fn read_u32(b: &[u8]) -> u64 {
    u32::from_le_bytes(b[..4].try_into().unwrap()) as u64
}

pub fn hash64(key: &[u8]) -> u64 {
    let len = key.len();
    let mut seed = P0 ^ (len as u64).wrapping_mul(P1);
    let mut p = key;
    while p.len() >= 16 {
        seed = mum(read_u64(p) ^ P1, read_u64(&p[8..]) ^ seed);
        p = &p[16..];
    }
    let (a, b) = match p.len() {
        0 => (0, 0),
        1..=3 => {
            let a =
                ((p[0] as u64) << 16) | ((p[p.len() >> 1] as u64) << 8) | (p[p.len() - 1] as u64);
            (a, 0)
        }
        4..=7 => (read_u32(p), read_u32(&p[p.len() - 4..])),
        _ => (read_u64(p), read_u64(&p[p.len() - 8..])),
    };
    let h = mum(a ^ P1, b ^ seed);
    mum(h ^ P2, h.rotate_left(29) ^ len as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_and_distinct() {
        assert_eq!(hash64(b"hello"), hash64(b"hello"));
        assert_ne!(hash64(b"hello"), hash64(b"hellp"));
        assert_ne!(hash64(b""), hash64(b"a"));
        assert_ne!(hash64(b"0123456789abcdef"), hash64(b"0123456789abcdeg"));
        // top bits should vary across sequential keys
        let mut tops = std::collections::HashSet::new();
        for i in 0..4096u32 {
            tops.insert(hash64(&i.to_le_bytes()) >> 44); // 20-bit prefix
        }
        assert!(tops.len() > 4000, "poor top-bit mixing: {}", tops.len());
    }
}
