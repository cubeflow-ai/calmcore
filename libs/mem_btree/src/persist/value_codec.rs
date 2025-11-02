use roaring::RoaringBitmap;
use std::error::Error;

use super::num_ser;

/// Encode a list of u32 doc IDs into bytes with a leading marker byte.
/// - 0: delta-encoded u32 sequence (big-endian, mem_btree::persist::num_ser::u32_coder)
/// - 1: RoaringBitmap native serialization
/// Returns Vec<u8> on success.
pub fn encode_roaring_from_u32s(ids: &[u32]) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::new();
    if ids.len() < 1000 {
        // marker 0 => delta encoding
        out.push(0u8);
        num_ser::u32_coder::write_delta(&mut out, &ids.to_vec())?;
    } else {
        // marker 1 => roaring native
        out.push(1u8);
        let rb = RoaringBitmap::from_iter(ids.iter().copied());
        // roaring serialize_into returns io::Result
        rb.serialize_into(&mut out)?;
    }
    Ok(out)
}

/// Decode bytes encoded by `encode_roaring_from_u32s` back into a RoaringBitmap.
/// Accepts empty slice as empty bitmap.
pub fn decode_roaring_from_bytes(
    data: &[u8],
) -> std::result::Result<RoaringBitmap, Box<dyn Error>> {
    if data.is_empty() {
        return Ok(RoaringBitmap::new());
    }

    let marker = data[0];
    let mut data_slice = &data[1..];
    match marker {
        0 => {
            let ids = num_ser::u32_coder::read_delta(&data_slice);
            Ok(RoaringBitmap::from_iter(ids))
        }
        1 => RoaringBitmap::deserialize_from(&mut data_slice)
            .map_err(|e| Box::new(e) as Box<dyn Error>),
        _ => Err(format!("Unknown marker byte: {}", marker).into()),
    }
}

/// Encode a RoaringBitmap into bytes with a leading marker, choosing the smaller form
/// between: 0 + sorted u32s, or 1 + roaring native format.
pub fn encode_roaring_from_bitmap(bitmap: &RoaringBitmap) -> Vec<u8> {
    // Estimate sizes
    let ids: Vec<u32> = bitmap.iter().collect();
    let vec_size = 1 + ids.len() * 4;
    let bitmap_size = 1 + bitmap.serialized_size();

    let mut buf = Vec::new();
    if vec_size <= bitmap_size {
        buf.push(0u8);
        for id in ids {
            buf.extend_from_slice(&id.to_be_bytes());
        }
    } else {
        buf.push(1u8);
        if let Err(_) = bitmap.serialize_into(&mut buf) {
            // Fallback to plain ids if roaring serialization fails for any reason
            buf.clear();
            buf.push(0u8);
            for id in bitmap.iter() {
                buf.extend_from_slice(&id.to_be_bytes());
            }
        }
    }
    buf
}
