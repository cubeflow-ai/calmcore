use num_traits::{abs, FromPrimitive, PrimInt, ToPrimitive};
use types::*;

mod types {
    pub const CUSTOM: u8 = 0;
    pub const DELTA: u8 = 1;
    pub const SAME: u8 = 2;
    pub const ARITHMETIC: u8 = 3;
}

pub mod u16_coder {
    use super::types::*;
    use crate::persist::zigzag::{self, BufferRead};
    use std::io::Write;

    pub fn write<W: Write>(writer: &mut W, values: &[u16]) -> std::io::Result<()> {
        match super::guess_type(values).unwrap_or(CUSTOM) {
            SAME => {
                writer.write_all(&[SAME])?;
                zigzag::write_u32(values.len() as u32, writer)?;
                zigzag::write_u16(values[0], writer)?;
            }
            ARITHMETIC => {
                writer.write_all(&[ARITHMETIC])?;
                zigzag::write_u32(values.len() as u32, writer)?;
                zigzag::write_u16(values[0], writer)?;
                zigzag::write_i32(values[1] as i32 - values[0] as i32, writer)?;
            }
            DELTA => {
                writer.write_all(&[DELTA])?;
                zigzag::write_u32(values.len() as u32, writer)?;
                if values.len() == 0 {
                    return Ok(());
                }
                //writer first value
                zigzag::write_u16(values[0], writer)?;
                for i in 1..values.len() {
                    zigzag::write_i32(values[i] as i32 - values[i - 1] as i32, writer)?;
                }
            }
            CUSTOM => {
                writer.write_all(&[CUSTOM])?;
                zigzag::write_u32(values.len() as u32, writer)?;
                if values.len() == 0 {
                    return Ok(());
                }
                for i in values {
                    zigzag::write_u16(*i, writer)?;
                }
            }
            _ => unreachable!(),
        };
        Ok(())
    }

    pub fn read<B: BufferRead>(buf: &B, pos: &mut usize) -> Vec<u16> {
        let arr_type = buf.get_byte(*pos);
        *pos += 1;
        match arr_type {
            SAME => {
                let len = zigzag::read_u32(buf, pos);
                let value = zigzag::read_u16(buf, pos);
                vec![value; len as usize]
            }
            ARITHMETIC => {
                let len = zigzag::read_u32(buf, pos);
                let value = zigzag::read_u16(buf, pos);
                let diff = zigzag::read_i32(buf, pos);
                (0..len as i32)
                    .into_iter()
                    .map(|i| (value as i32 + i * diff) as u16)
                    .collect()
            }
            DELTA => {
                let len = zigzag::read_u32(buf, pos);
                let mut value = zigzag::read_u16(buf, pos);
                let mut result = Vec::with_capacity(len as usize);
                result.push(value);
                for _ in 1..len {
                    value = (zigzag::read_i32(buf, pos) + value as i32) as u16;
                    result.push(value);
                }
                result
            }
            CUSTOM => {
                let len = zigzag::read_u32(buf, pos);
                let mut result = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    result.push(zigzag::read_u16(buf, pos));
                }
                result
            }
            _ => unreachable!("unknow arr_type:{}", arr_type),
        }
    }
}

pub mod u32_coder {
    use crate::persist::zigzag::{self, BufferRead};
    use std::io::Write;

    pub fn write_delta<W: Write>(writer: &mut W, values: &[u32]) -> std::io::Result<()> {
        zigzag::write_u32(values.len() as u32, writer)?;
        if values.is_empty() {
            return Ok(());
        }
        //writer first value
        zigzag::write_u32(values[0], writer)?;
        for i in 1..values.len() {
            zigzag::write_u32(values[i].wrapping_sub(values[i - 1]), writer)?;
        }
        Ok(())
    }

    pub fn read_delta<B: BufferRead>(buf: &B) -> Vec<u32> {
        let mut pos = 0;
        let len = zigzag::read_u32(buf, &mut pos);
        if len == 0 {
            return Vec::new();
        }
        let mut value = zigzag::read_u32(buf, &mut pos);
        let mut result = Vec::with_capacity(len as usize);
        result.push(value);
        for _ in 1..len {
            value = zigzag::read_u32(buf, &mut pos).wrapping_add(value);
            result.push(value);
        }
        result
    }

    pub fn read_delta_pos<B: BufferRead>(buf: &B, pos: &mut usize) -> Vec<u32> {
        let len = zigzag::read_u32(buf, pos);
        if len == 0 {
            return Vec::new();
        }
        let mut value = zigzag::read_u32(buf, pos);
        let mut result = Vec::with_capacity(len as usize);
        result.push(value);
        for _ in 1..len {
            value = zigzag::read_u32(buf, pos).wrapping_add(value);
            result.push(value);
        }
        result
    }
}

pub mod i64_coder {
    use super::types::*;
    use crate::persist::zigzag::{self, BufferRead};
    use std::io::Write;

    pub fn write_delta<W: Write>(writer: &mut W, values: &[i64]) -> std::io::Result<()> {
        zigzag::write_u32(values.len() as u32, writer)?;
        if values.len() == 0 {
            return Ok(());
        }
        //writer first value
        zigzag::write_i64(values[0], writer)?;
        for i in 1..values.len() {
            zigzag::write_i64(values[i] - values[i - 1], writer)?;
        }
        Ok(())
    }

    pub fn read_delta<B: BufferRead>(buf: &B) -> Vec<i64> {
        let mut pos = 0;
        let len = zigzag::read_u32(buf, &mut pos);
        if len == 0 {
            return Vec::new();
        }
        let mut value = zigzag::read_i64(buf, &mut pos);
        let mut result = Vec::with_capacity(len as usize);
        result.push(value);
        for _ in 1..len {
            value = zigzag::read_i64(buf, &mut pos) + value;
            result.push(value);
        }
        result
    }

    /// Read a delta-encoded i64 array from `buf` starting at `*pos`, and advance `pos`.
    /// Format: len(u32 varint) + first(i64 zigzag) + (len-1) diffs(i64 zigzag)
    pub fn read_delta_pos<B: BufferRead>(buf: &B, pos: &mut usize) -> Vec<i64> {
        let len = zigzag::read_u32(buf, pos);
        if len == 0 {
            return Vec::new();
        }
        let mut value = zigzag::read_i64(buf, pos);
        let mut result = Vec::with_capacity(len as usize);
        result.push(value);
        for _ in 1..len {
            value = zigzag::read_i64(buf, pos) + value;
            result.push(value);
        }
        result
    }

    pub fn write<W: Write>(writer: &mut W, values: &[i64]) -> std::io::Result<()> {
        match super::guess_type(values).unwrap_or(CUSTOM) {
            SAME => {
                writer.write_all(&[SAME])?;
                zigzag::write_u32(values.len() as u32, writer)?;
                zigzag::write_i64(values[0], writer)?;
            }
            ARITHMETIC => {
                writer.write_all(&[ARITHMETIC])?;
                zigzag::write_u32(values.len() as u32, writer)?;
                zigzag::write_i64(values[0], writer)?;
                zigzag::write_i64(values[1] - values[0], writer)?;
            }
            DELTA => {
                writer.write_all(&[DELTA])?;
                zigzag::write_u32(values.len() as u32, writer)?;
                if values.len() == 0 {
                    return Ok(());
                }
                //writer first value
                zigzag::write_i64(values[0], writer)?;
                for i in 1..values.len() {
                    zigzag::write_i64(values[i] - values[i - 1], writer)?;
                }
            }
            CUSTOM => {
                writer.write_all(&[CUSTOM])?;
                zigzag::write_u32(values.len() as u32, writer)?;
                if values.len() == 0 {
                    return Ok(());
                }
                for i in values {
                    zigzag::write_i64(*i, writer)?;
                }
            }
            _ => unreachable!(),
        };
        Ok(())
    }

    pub fn read<B: BufferRead>(buf: &B, pos: &mut usize) -> Vec<i64> {
        let arr_type = buf.get_byte(*pos);
        *pos += 1;
        match arr_type {
            SAME => {
                let len = zigzag::read_u32(buf, pos);
                let value = zigzag::read_i64(buf, pos);
                vec![value; len as usize]
            }
            ARITHMETIC => {
                let len = zigzag::read_u32(buf, pos);
                let value = zigzag::read_i64(buf, pos);
                let diff = zigzag::read_i64(buf, pos);
                (0..len as i64)
                    .into_iter()
                    .map(|i| value + i * diff)
                    .collect()
            }
            DELTA => {
                // Delegate to delta reader that advances pos
                read_delta_pos(buf, pos)
            }
            _ => {
                let len = zigzag::read_u32(buf, pos);
                let mut result = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    result.push(zigzag::read_i64(buf, pos));
                }
                result
            }
        }
    }
}

/// Guess the type of the given values.
/// if the values are all the same, return `types::SAME`
/// if the values are all arithmetic, return `types::ARITHMETIC`
/// otherwise, return None means `types::CUSTOM`
fn guess_type<T>(values: &[T]) -> Option<u8>
where
    T: PrimInt + ToPrimitive + FromPrimitive,
{
    if values.len() <= 3 {
        return None;
    }

    let mut all_arithmetic: i64 = 0;
    let mut max_arithmetic: i64 = 0;
    let mut monotonic = 0;

    for i in 1..values.len() {
        let v1 = values[i].to_i64()?;
        let v2 = values[i - 1].to_i64()?;

        // if the values are not sorted, return None
        if v1 < v2 {
            monotonic += 1;
        } else {
            monotonic -= 1;
        }

        let diff = v1 - v2;
        all_arithmetic += diff;
        max_arithmetic = max_arithmetic.max(diff);
    }

    // is monotonic

    if abs(monotonic) as usize != values.len() - 1 {
        return None;
    }

    if all_arithmetic == 0 {
        Some(SAME)
    } else if all_arithmetic == max_arithmetic * (values.len() as i64 - 1) {
        Some(ARITHMETIC)
    } else if all_arithmetic < 65535 {
        Some(DELTA)
    } else {
        None
    }
}

#[cfg(test)]
mod test {
    use super::{i64_coder, u16_coder};

    #[test]
    fn test_guess_type() {
        let same = vec![1, 1, 1, 1, 1];
        assert_eq!(super::guess_type(&same), Some(super::types::SAME));

        let arithmetic = vec![1, 2, 3, 4, 5];
        assert_eq!(
            super::guess_type(&arithmetic),
            Some(super::types::ARITHMETIC)
        );

        let delta = vec![1, 3, 6, 10, 15];
        assert_eq!(super::guess_type(&delta), Some(super::types::DELTA));

        let custom = vec![1, 2];
        assert_eq!(super::guess_type(&custom), None);

        let custom2 = vec![1, 2000, 300000, 400, 500, 600, 70, 8000, 9000, 100];
        assert_eq!(super::guess_type(&custom2), None);
    }

    #[test]
    fn test_u16() {
        let values = vec![
            vec![1, 1, 1, 1, 1],
            vec![1, 2, 3, 4, 5],
            vec![1, 3, 6, 10, 15],
            vec![
                1, 2000, 30000, 400, 500, 600, 70, 8000, 9000, 100, 1000, 7897, 1234, 5678, 9999,
            ],
        ];
        for values in values {
            let mut bytes = Vec::new();
            u16_coder::write(&mut bytes, &values).unwrap();

            assert!(
                values.len() * 2 >= bytes.len(),
                "values: {:?} values_len:{}  bytes:{}",
                values,
                values.len() * 2,
                bytes.len()
            );
            let result = u16_coder::read(&bytes.to_vec(), &mut 0);
            assert_eq!(values, result);
        }
    }

    #[test]
    fn test_i64() {
        let values = vec![
            vec![1, 1, 1, 1, 1],
            vec![1, 2, 3, 4, 5],
            vec![1, 3, 6, 10, 15],
            vec![
                1, 2000, 30000, 400, 500, 600, 70, 8000, 9000, 100, 1000, 7897, 1234, 5678, 9999,
            ],
        ];
        for values in values {
            let mut bytes = Vec::new();
            i64_coder::write(&mut bytes, &values).unwrap();

            assert!(
                values.len() * 8 >= bytes.len(),
                "values: {:?} values_len:{}  bytes:{}",
                values,
                values.len() * 8,
                bytes.len()
            );
            let result = i64_coder::read(&bytes.to_vec(), &mut 0);
            assert_eq!(values, result);
        }
    }
}
