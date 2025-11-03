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
/// between: 0 + delta-encoded u32s, or 1 + roaring native format.
pub fn encode_roaring_from_bitmap(bitmap: &RoaringBitmap) -> Vec<u8> {
    // Estimate sizes - need to check actual delta-encoded size
    let ids: Vec<u32> = bitmap.iter().collect();

    // Try delta encoding
    let mut delta_buf = Vec::new();
    delta_buf.push(0u8);
    if let Ok(_) = num_ser::u32_coder::write_delta(&mut delta_buf, &ids) {
        // Try roaring native
        let mut roaring_buf = Vec::new();
        roaring_buf.push(1u8);
        if let Ok(_) = bitmap.serialize_into(&mut roaring_buf) {
            // Choose the smaller one
            if delta_buf.len() <= roaring_buf.len() {
                return delta_buf;
            } else {
                return roaring_buf;
            }
        } else {
            // Roaring serialization failed, use delta
            return delta_buf;
        }
    } else {
        // Delta encoding failed (shouldn't happen), fallback to roaring
        let mut buf = Vec::new();
        buf.push(1u8);
        let _ = bitmap.serialize_into(&mut buf);
        return buf;
    }
}
