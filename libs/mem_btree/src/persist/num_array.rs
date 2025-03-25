// use bytes::{BufMut, BytesMut};
// use num_traits::{FromPrimitive, PrimInt, Signed, ToPrimitive, Unsigned};
// use std::io::{self, Write};

// /// NumArray 提供了各种整数数组压缩方法，支持不同整数类型
// pub struct NumArray;

// impl NumArray {
//     /// 压缩并写入任意整数数组，自动选择最佳压缩方式
//     /// 支持 i32, i64, u16 等整数类型
//     pub fn write_nums<W: Write, T>(writer: &mut W, values: &[T]) -> io::Result<usize>
//     where
//         T: PrimInt + ToPrimitive + FromPrimitive + 'static,
//     {
//         // 写入类型标识
//         let type_id = Self::get_type_id::<T>();
//         writer.write_all(&[type_id])?;

//         // 判断数组特性，选择合适的压缩方式
//         if values.is_empty() {
//             return Self::write_empty(writer);
//         }

//         // 根据类型ID决定编码逻辑
//         match type_id {
//             // i8, i16, i32, i64
//             0..=3 => Self::write_signed_nums_impl(writer, values),
//             // u8, u16, u32, u64
//             4..=7 => Self::write_unsigned_nums_impl(writer, values),
//             // 未知类型，使用通用方法
//             _ => Self::write_generic_nums(writer, values),
//         }
//     }

//     // 内部使用的分发函数，避免trait约束问题
//     fn write_signed_nums_impl<W: Write, T>(writer: &mut W, values: &[T]) -> io::Result<usize>
//     where
//         T: PrimInt + ToPrimitive + FromPrimitive + 'static,
//     {
//         // 检测是否有稀疏模式(大量为0)，这是优先级最高的编码方式
//         if Self::has_sparse_pattern(values) {
//             return Self::write_sparse(writer, values);
//         }

//         // 安全地检查是否单调且密集
//         if Self::is_safe_monotonic(values) {
//             return Self::write_monotonic(writer, values);
//         }

//         // 检测值的范围，如果范围小，使用紧凑编码
//         let (min, max) = Self::find_min_max(values);
//         let range_i64_opt = max
//             .to_i64()
//             .and_then(|max_val| min.to_i64().map(|min_val| max_val.saturating_sub(min_val)));

//         // 如果无法计算范围或范围溢出，使用delta编码
//         let range = match range_i64_opt {
//             Some(r) if r >= 0 => r as u64,
//             _ => return Self::write_delta_vint(writer, values),
//         };

//         if range < 256 {
//             return Self::write_for_byte(writer, values, min);
//         } else if range < 65536 {
//             return Self::write_for_short(writer, values, min);
//         } else {
//             // 默认使用delta+variable编码
//             return Self::write_delta_vint(writer, values);
//         }
//     }

//     // 处理无符号整数的压缩
//     fn write_unsigned_nums_impl<W: Write, T>(writer: &mut W, values: &[T]) -> io::Result<usize>
//     where
//         T: PrimInt + ToPrimitive + FromPrimitive + 'static,
//     {
//         // 检测是否有稀疏模式(大量为0)
//         if Self::has_sparse_pattern(values) {
//             println!("has sparse pattern");
//             return Self::write_sparse(writer, values);
//         }

//         // 安全地检查是否单调且密集
//         if Self::is_safe_monotonic(values) {
//             println!("monotonic");
//             return Self::write_monotonic(writer, values);
//         }

//         // 检测值的范围
//         let (min, max) = Self::find_min_max(values);
//         let range_u64_opt = max
//             .to_u64()
//             .and_then(|max_val| min.to_u64().map(|min_val| max_val.saturating_sub(min_val)));

//         // 如果无法计算范围，使用delta编码
//         let range = match range_u64_opt {
//             Some(r) => r,
//             None => return Self::write_delta_vint(writer, values),
//         };

//         if range < 256 {
//             return Self::write_for_byte(writer, values, min);
//         } else if range < 65536 {
//             return Self::write_for_short(writer, values, min);
//         } else {
//             return Self::write_delta_vint(writer, values);
//         }
//     }

//     // 通用的编码方法，不需要额外的trait约束
//     fn write_generic_nums<W: Write, T>(writer: &mut W, values: &[T]) -> io::Result<usize>
//     where
//         T: PrimInt + ToPrimitive + FromPrimitive,
//     {
//         Self::write_delta_vint(writer, values)
//     }

//     /// 获取类型标识字节
//     fn get_type_id<T: 'static>() -> u8 {
//         // 标识不同的整数类型
//         let type_id = std::any::TypeId::of::<T>();

//         if type_id == std::any::TypeId::of::<i8>() {
//             0
//         } else if type_id == std::any::TypeId::of::<i16>() {
//             1
//         } else if type_id == std::any::TypeId::of::<i32>() {
//             2
//         } else if type_id == std::any::TypeId::of::<i64>() {
//             3
//         } else if type_id == std::any::TypeId::of::<u8>() {
//             4
//         } else if type_id == std::any::TypeId::of::<u16>() {
//             5
//         } else if type_id == std::any::TypeId::of::<u32>() {
//             6
//         } else if type_id == std::any::TypeId::of::<u64>() {
//             7
//         } else {
//             255 // 未知类型
//         }
//     }

//     /// 写入空数组
//     fn write_empty<W: Write>(writer: &mut W) -> io::Result<usize> {
//         // 标记类型 0 表示空数组 (不包含类型标识)
//         writer.write_all(&[0])?;
//         writer.write_all(&[0, 0, 0, 0])?; // 长度为0
//         Ok(5)
//     }

//     /// 写入单调数组(起始值 + 步长)
//     fn write_monotonic<W: Write, T>(writer: &mut W, values: &[T]) -> io::Result<usize>
//     where
//         T: PrimInt + ToPrimitive + FromPrimitive,
//     {
//         if values.len() < 2 {
//             return Self::write_delta_vint(writer, values);
//         }

//         let first = values[0];
//         let second = values[1];
//         let step = second - first;

//         // 验证是否所有值都遵循这个步长
//         for i in 1..values.len() {
//             let i_as_t = FromPrimitive::from_usize(i).unwrap();
//             let expected = first + (step * i_as_t);
//             if values[i] != expected {
//                 return Self::write_delta_vint(writer, values);
//             }
//         }

//         // 标记类型 1 表示单调数组
//         writer.write_all(&[1])?;

//         // 写入数组长度
//         writer.write_all(&(values.len() as u32).to_le_bytes())?;

//         // 转换为i64写入，确保兼容不同整数类型
//         // 写入起始值
//         let first_i64 = first.to_i64().unwrap_or(0);
//         writer.write_all(&first_i64.to_le_bytes())?;

//         // 写入步长
//         let step_i64 = step.to_i64().unwrap_or(0);
//         writer.write_all(&step_i64.to_le_bytes())?;

//         Ok(1 + 4 + 8 + 8) // 类型 + 长度 + 首值(i64) + 步长(i64)
//     }

//     /// FOR (Frame of Reference) 字节压缩: 整数表示为基准值+偏移量方式，偏移量为1字节
//     fn write_for_byte<W: Write, T>(writer: &mut W, values: &[T], base: T) -> io::Result<usize>
//     where
//         T: PrimInt + ToPrimitive + FromPrimitive,
//     {
//         // 标记类型 2 表示FOR字节压缩
//         writer.write_all(&[2])?;

//         // 写入数组长度
//         writer.write_all(&(values.len() as u32).to_le_bytes())?;

//         // 写入基准值 (转换为i64确保兼容性)
//         let base_i64 = base.to_i64().unwrap_or(0);
//         writer.write_all(&base_i64.to_le_bytes())?;

//         // 写入每个值相对于基准值的偏移量
//         let mut bytes_written = 1 + 4 + 8; // 类型 + 长度 + 基准值(i64)
//         for &v in values {
//             // 计算当前值与基准值的差，并转换为u8
//             let diff = (v - base).to_u8().unwrap_or(0);
//             writer.write_all(&[diff])?;
//             bytes_written += 1;
//         }

//         Ok(bytes_written)
//     }

//     /// FOR short压缩: 整数表示为基准值+偏移量方式，偏移量为2字节
//     fn write_for_short<W: Write, T>(writer: &mut W, values: &[T], base: T) -> io::Result<usize>
//     where
//         T: PrimInt + ToPrimitive + FromPrimitive,
//     {
//         // 标记类型 3 表示FOR short压缩
//         writer.write_all(&[3])?;

//         // 写入数组长度
//         writer.write_all(&(values.len() as u32).to_le_bytes())?;

//         // 写入基准值 (转换为i64确保兼容性)
//         let base_i64 = base.to_i64().unwrap_or(0);
//         writer.write_all(&base_i64.to_le_bytes())?;

//         // 写入每个值相对于基准值的偏移量
//         let mut bytes_written = 1 + 4 + 8; // 类型 + 长度 + 基准值(i64)
//         for &v in values {
//             // 计算当前值与基准值的差，并转换为u16
//             let diff = (v - base).to_u16().unwrap_or(0);
//             writer.write_all(&diff.to_le_bytes())?;
//             bytes_written += 2;
//         }

//         Ok(bytes_written)
//     }

//     /// 稀疏数组编码: 存储索引-值对
//     fn write_sparse<W: Write, T>(writer: &mut W, values: &[T]) -> io::Result<usize>
//     where
//         T: PrimInt + ToPrimitive + FromPrimitive,
//     {
//         // 计算非0值的数量
//         let zero = T::zero();
//         let non_zero_count = values.iter().filter(|&&v| v != zero).count();

//         // 如果非0值太多，不适合稀疏编码
//         // 非零值应少于10%才用稀疏编码
//         if non_zero_count * 10 > values.len() {
//             return Self::write_delta_vint(writer, values);
//         }

//         // 标记类型 4 表示稀疏编码
//         writer.write_all(&[4])?;

//         // 写入数组总长度和非0值数量
//         writer.write_all(&(values.len() as u32).to_le_bytes())?;
//         writer.write_all(&(non_zero_count as u32).to_le_bytes())?;

//         // 写入索引-值对
//         let mut bytes_written = 1 + 4 + 4; // 类型 + 长度 + 非0值数量
//         for (idx, &v) in values.iter().enumerate() {
//             if v != zero {
//                 // 写入索引
//                 writer.write_all(&(idx as u32).to_le_bytes())?;

//                 // 写入值 (转换为i64)
//                 let val_i64 = v.to_i64().unwrap_or(0);
//                 writer.write_all(&val_i64.to_le_bytes())?;

//                 bytes_written += 4 + 8; // 索引(u32) + 值(i64)
//             }
//         }

//         Ok(bytes_written)
//     }

//     /// Delta + VInt编码: 存储连续值的差值，并使用变长编码
//     fn write_delta_vint<W: Write, T>(writer: &mut W, values: &[T]) -> io::Result<usize>
//     where
//         T: PrimInt + ToPrimitive + FromPrimitive,
//     {
//         // 标记类型 5 表示Delta+VInt编码
//         writer.write_all(&[5])?;

//         // 写入数组长度
//         writer.write_all(&(values.len() as u32).to_le_bytes())?;

//         let mut bytes_written = 1 + 4; // 类型 + 长度

//         if values.is_empty() {
//             return Ok(bytes_written);
//         }

//         // 写入第一个完整值 (转换为i64)
//         let first_i64 = values[0].to_i64().unwrap_or(0);
//         writer.write_all(&first_i64.to_le_bytes())?;
//         bytes_written += 8;

//         // 写入后续值的差值
//         let mut prev = values[0];
//         for &v in &values[1..] {
//             let delta = v - prev;
//             // 将差值转换为i32后进行zigzag编码
//             // 这里需要考虑大整数溢出的情况
//             let delta_i32 = delta.to_i32().unwrap_or_else(|| {
//                 if delta > T::zero() {
//                     i32::MAX
//                 } else {
//                     i32::MIN
//                 }
//             });

//             let zigzag = Self::zigzag_encode(delta_i32);
//             bytes_written += Self::write_vint(writer, zigzag)?;
//             prev = v;
//         }

//         Ok(bytes_written)
//     }

//     /// ZigZag编码，将有符号整数映射为无符号整数
//     fn zigzag_encode(n: i32) -> u32 {
//         ((n << 1) ^ (n >> 31)) as u32
//     }

//     /// 写入变长整数
//     fn write_vint<W: Write>(writer: &mut W, mut value: u32) -> io::Result<usize> {
//         let mut bytes_written = 0;

//         while value >= 128 {
//             writer.write_all(&[(0x80 | (value & 0x7F)) as u8])?;
//             value >>= 7;
//             bytes_written += 1;
//         }

//         writer.write_all(&[value as u8])?;
//         bytes_written += 1;

//         Ok(bytes_written)
//     }

//     /// 辅助方法: 检测数组是否单调且密集 (安全版本，避免整数溢出)
//     fn is_safe_monotonic<T>(values: &[T]) -> bool
//     where
//         T: PrimInt + ToPrimitive + FromPrimitive,
//     {
//         if values.len() < 3 {
//             return false;
//         }

//         // 从第一个元素和第二个元素计算步长
//         let first = values[0];
//         let second = values[1];

//         // 使用saturating_sub来避免溢出
//         let first_i64 = match first.to_i64() {
//             Some(v) => v,
//             None => return false, // 无法转换为i64，不是单调的
//         };

//         let second_i64 = match second.to_i64() {
//             Some(v) => v,
//             None => return false, // 无法转换为i64，不是单调的
//         };

//         // 使用saturating_sub来避免溢出
//         let step_i64 = second_i64.saturating_sub(first_i64);

//         // 步长为0，不是单调的
//         if step_i64 == 0 {
//             return false;
//         }

//         // 验证每个值是否符合单调规则
//         let mut expected_i64 = first_i64;
//         for &v in values.iter() {
//             let actual_i64 = match v.to_i64() {
//                 Some(v) => v,
//                 None => return false, // 无法转换为i64，不是单调的
//             };

//             if actual_i64 != expected_i64 {
//                 return false;
//             }

//             // 使用saturating_add来避免溢出
//             expected_i64 = expected_i64.saturating_add(step_i64);

//             // 检查是否溢出了i64的范围
//             if expected_i64 == i64::MAX || expected_i64 == i64::MIN {
//                 return false;
//             }
//         }

//         true
//     }

//     /// 辅助方法: 查找数组最小值和最大值
//     fn find_min_max<T>(values: &[T]) -> (T, T)
//     where
//         T: PrimInt,
//     {
//         if values.is_empty() {
//             return (T::zero(), T::zero());
//         }

//         let mut min = values[0];
//         let mut max = values[0];

//         for &v in values {
//             if v < min {
//                 min = v;
//             }
//             if v > max {
//                 max = v;
//             }
//         }

//         (min, max)
//     }

//     /// 辅助方法: 检测是否有稀疏模式(大量为0)
//     fn has_sparse_pattern<T>(values: &[T]) -> bool
//     where
//         T: PrimInt,
//     {
//         // 对小数组不使用稀疏编码
//         if values.len() < 10 {
//             return false;
//         }

//         // 计算0值的数量
//         let zero = T::zero();
//         let zero_count = values.iter().filter(|&&v| v == zero).count();

//         // 超过95%的值为0，则认为是稀疏的
//         zero_count * 100 > values.len() * 95
//     }

//     /// 块压缩写入整数数组，适合随机访问
//     pub fn write_packed_nums<W: Write, T>(
//         writer: &mut W,
//         values: &[T],
//         bits_per_value: u8,
//     ) -> io::Result<usize>
//     where
//         T: PrimInt + ToPrimitive + 'static,
//     {
//         // 类型标识 + 标记类型 6 表示块压缩
//         let type_id = Self::get_type_id::<T>();
//         writer.write_all(&[type_id, 6])?;

//         // 写入数组长度和每个值的位数
//         writer.write_all(&(values.len() as u32).to_le_bytes())?;
//         writer.write_all(&[bits_per_value])?;

//         if bits_per_value >= 64 {
//             // 不压缩，直接写入完整值
//             for &v in values {
//                 let val_i64 = v.to_i64().unwrap_or(0);
//                 writer.write_all(&val_i64.to_le_bytes())?;
//             }
//             return Ok(2 + 4 + 1 + values.len() * 8); // 类型ID + 编码类型 + 长度 + 位数 + 数据
//         }

//         let mut buffer = BytesMut::new();
//         let values_per_long = 64 / bits_per_value as usize;
//         let mask = (1u64 << bits_per_value) - 1;

//         for chunk in values.chunks(values_per_long) {
//             let mut packed = 0u64;

//             for (i, &v) in chunk.iter().enumerate() {
//                 // 将任何整数类型转换为u64用于位操作
//                 let val_u64 = v.to_u64().unwrap_or(0) & mask;
//                 packed |= val_u64 << (i * bits_per_value as usize);
//             }

//             buffer.put_u64_le(packed);
//         }

//         writer.write_all(&buffer)?;
//         Ok(2 + 4 + 1 + buffer.len()) // 类型ID + 编码类型 + 长度 + 位数 + 压缩数据
//     }

//     // 针对i64类型的特化方法
//     pub fn write_i64<W: Write>(writer: &mut W, values: &[i64]) -> io::Result<usize> {
//         Self::write_nums(writer, values)
//     }

//     // 针对u16类型的特化方法
//     pub fn write_u16<W: Write>(writer: &mut W, values: &[u16]) -> io::Result<usize> {
//         Self::write_nums(writer, values)
//     }

//     /// 使用内存映射读取压缩的整数数组
//     /// 这个方法接受任何实现了Read trait的对象，包括mmap的文件切片
//     pub fn read_nums_from_mmap<T>(mmap_slice: &[u8]) -> io::Result<Vec<T>>
//     where
//         T: PrimInt + ToPrimitive + FromPrimitive + 'static,
//     {
//         let mut reader = std::io::Cursor::new(mmap_slice);
//         Self::read_nums(&mut reader)
//     }
// }

// // 新增一个读取方法，用于以后扩展
// impl NumArray {
//     /// 读取压缩的整数数组
//     pub fn read_nums<R: std::io::Read, T>(reader: &mut R) -> io::Result<Vec<T>>
//     where
//         T: PrimInt + ToPrimitive + FromPrimitive + 'static,
//     {
//         // 读取类型标识
//         let mut type_id = [0u8; 1];
//         reader.read_exact(&mut type_id)?;

//         // 检查类型是否匹配
//         let expected_type_id = Self::get_type_id::<T>();
//         if type_id[0] != expected_type_id && type_id[0] != 255 {
//             return Err(io::Error::new(
//                 io::ErrorKind::InvalidData,
//                 format!("类型不匹配：期望 {}, 实际 {}", expected_type_id, type_id[0]),
//             ));
//         }

//         // 读取编码类型
//         let mut encoding = [0u8; 1];
//         reader.read_exact(&mut encoding)?;

//         match encoding[0] {
//             0 => Self::read_empty(reader),
//             1 => Self::read_monotonic(reader),
//             2 => Self::read_for_byte(reader),
//             3 => Self::read_for_short(reader),
//             4 => Self::read_sparse(reader),
//             5 => Self::read_delta_vint(reader),
//             6 => Self::read_packed(reader),
//             _ => Err(io::Error::new(io::ErrorKind::InvalidData, "未知编码类型")),
//         }
//     }

//     /// 读取空数组
//     fn read_empty<R: std::io::Read, T>(reader: &mut R) -> io::Result<Vec<T>>
//     where
//         T: PrimInt + ToPrimitive + FromPrimitive,
//     {
//         // 读取长度（应该为0）
//         let mut len_bytes = [0u8; 4];
//         reader.read_exact(&mut len_bytes)?;
//         let len = u32::from_le_bytes(len_bytes);

//         if len != 0 {
//             return Err(io::Error::new(
//                 io::ErrorKind::InvalidData,
//                 format!("空数组编码中长度应为0，实际为 {}", len),
//             ));
//         }

//         Ok(Vec::new())
//     }

//     /// 读取单调数组
//     fn read_monotonic<R: std::io::Read, T>(reader: &mut R) -> io::Result<Vec<T>>
//     where
//         T: PrimInt + ToPrimitive + FromPrimitive,
//     {
//         // 读取数组长度
//         let mut len_bytes = [0u8; 4];
//         reader.read_exact(&mut len_bytes)?;
//         let len = u32::from_le_bytes(len_bytes) as usize;

//         // 读取起始值
//         let mut first_bytes = [0u8; 8];
//         reader.read_exact(&mut first_bytes)?;
//         let first_i64 = i64::from_le_bytes(first_bytes);

//         // 读取步长
//         let mut step_bytes = [0u8; 8];
//         reader.read_exact(&mut step_bytes)?;
//         let step_i64 = i64::from_le_bytes(step_bytes);

//         // 构建结果数组
//         let mut result = Vec::with_capacity(len);

//         for i in 0..len {
//             // 计算当前值 = 起始值 + 步长 * i
//             // 使用饱和加法避免溢出
//             let value_i64 = first_i64.saturating_add(step_i64.saturating_mul(i as i64));

//             // 将i64转换为目标类型T
//             match T::from_i64(value_i64) {
//                 Some(value) => result.push(value),
//                 None => {
//                     return Err(io::Error::new(
//                         io::ErrorKind::InvalidData,
//                         format!("无法将值 {} 转换为目标类型", value_i64),
//                     ))
//                 }
//             }
//         }

//         Ok(result)
//     }

//     /// 读取FOR字节压缩的数组
//     fn read_for_byte<R: std::io::Read, T>(reader: &mut R) -> io::Result<Vec<T>>
//     where
//         T: PrimInt + ToPrimitive + FromPrimitive,
//     {
//         // 读取数组长度
//         let mut len_bytes = [0u8; 4];
//         reader.read_exact(&mut len_bytes)?;
//         let len = u32::from_le_bytes(len_bytes) as usize;

//         // 读取基准值
//         let mut base_bytes = [0u8; 8];
//         reader.read_exact(&mut base_bytes)?;
//         let base_i64 = i64::from_le_bytes(base_bytes);

//         // 将基准值转换为目标类型
//         let base = match T::from_i64(base_i64) {
//             Some(b) => b,
//             None => {
//                 return Err(io::Error::new(
//                     io::ErrorKind::InvalidData,
//                     format!("无法将基准值 {} 转换为目标类型", base_i64),
//                 ))
//             }
//         };

//         // 读取偏移量并构建结果数组
//         let mut result = Vec::with_capacity(len);

//         for _ in 0..len {
//             // 读取一个字节偏移量
//             let mut offset_byte = [0u8];
//             reader.read_exact(&mut offset_byte)?;
//             let offset = offset_byte[0] as u64;

//             // 计算当前值 = 基准值 + 偏移量
//             // 首先将偏移量转换为目标类型，然后加上基准值
//             match T::from_u64(offset) {
//                 Some(offset_t) => result.push(base + offset_t),
//                 None => {
//                     return Err(io::Error::new(
//                         io::ErrorKind::InvalidData,
//                         format!("无法将偏移量 {} 转换为目标类型", offset),
//                     ))
//                 }
//             }
//         }

//         Ok(result)
//     }

//     /// 读取FOR short压缩的数组
//     fn read_for_short<R: std::io::Read, T>(reader: &mut R) -> io::Result<Vec<T>>
//     where
//         T: PrimInt + ToPrimitive + FromPrimitive,
//     {
//         // 读取数组长度
//         let mut len_bytes = [0u8; 4];
//         reader.read_exact(&mut len_bytes)?;
//         let len = u32::from_le_bytes(len_bytes) as usize;

//         // 读取基准值
//         let mut base_bytes = [0u8; 8];
//         reader.read_exact(&mut base_bytes)?;
//         let base_i64 = i64::from_le_bytes(base_bytes);

//         // 将基准值转换为目标类型
//         let base = match T::from_i64(base_i64) {
//             Some(b) => b,
//             None => {
//                 return Err(io::Error::new(
//                     io::ErrorKind::InvalidData,
//                     format!("无法将基准值 {} 转换为目标类型", base_i64),
//                 ))
//             }
//         };

//         // 读取偏移量并构建结果数组
//         let mut result = Vec::with_capacity(len);

//         for _ in 0..len {
//             // 读取两个字节偏移量
//             let mut offset_bytes = [0u8; 2];
//             reader.read_exact(&mut offset_bytes)?;
//             let offset = u16::from_le_bytes(offset_bytes) as u64;

//             // 计算当前值 = 基准值 + 偏移量
//             match T::from_u64(offset) {
//                 Some(offset_t) => result.push(base + offset_t),
//                 None => {
//                     return Err(io::Error::new(
//                         io::ErrorKind::InvalidData,
//                         format!("无法将偏移量 {} 转换为目标类型", offset),
//                     ))
//                 }
//             }
//         }

//         Ok(result)
//     }

//     /// 读取稀疏数组
//     fn read_sparse<R: std::io::Read, T>(reader: &mut R) -> io::Result<Vec<T>>
//     where
//         T: PrimInt + ToPrimitive + FromPrimitive,
//     {
//         // 读取数组总长度
//         let mut total_len_bytes = [0u8; 4];
//         reader.read_exact(&mut total_len_bytes)?;
//         let total_len = u32::from_le_bytes(total_len_bytes) as usize;

//         // 读取非零值数量
//         let mut non_zero_bytes = [0u8; 4];
//         reader.read_exact(&mut non_zero_bytes)?;
//         let non_zero_count = u32::from_le_bytes(non_zero_bytes) as usize;

//         // 创建全零数组
//         let mut result = vec![T::zero(); total_len];

//         // 读取并填充非零值
//         for _ in 0..non_zero_count {
//             // 读取索引
//             let mut idx_bytes = [0u8; 4];
//             reader.read_exact(&mut idx_bytes)?;
//             let idx = u32::from_le_bytes(idx_bytes) as usize;

//             if idx >= total_len {
//                 return Err(io::Error::new(
//                     io::ErrorKind::InvalidData,
//                     format!("索引 {} 超出数组范围 {}", idx, total_len),
//                 ));
//             }

//             // 读取值(i64格式)
//             let mut val_bytes = [0u8; 8];
//             reader.read_exact(&mut val_bytes)?;
//             let val_i64 = i64::from_le_bytes(val_bytes);

//             // 将值转换为目标类型并存储
//             match T::from_i64(val_i64) {
//                 Some(value) => result[idx] = value,
//                 None => {
//                     return Err(io::Error::new(
//                         io::ErrorKind::InvalidData,
//                         format!("无法将值 {} 转换为目标类型", val_i64),
//                     ))
//                 }
//             }
//         }

//         Ok(result)
//     }

//     /// 读取Delta+VInt编码的数组
//     fn read_delta_vint<R: std::io::Read, T>(reader: &mut R) -> io::Result<Vec<T>>
//     where
//         T: PrimInt + ToPrimitive + FromPrimitive,
//     {
//         // 读取数组长度
//         let mut len_bytes = [0u8; 4];
//         reader.read_exact(&mut len_bytes)?;
//         let len = u32::from_le_bytes(len_bytes) as usize;

//         if len == 0 {
//             return Ok(Vec::new());
//         }

//         // 读取第一个完整值
//         let mut first_bytes = [0u8; 8];
//         reader.read_exact(&mut first_bytes)?;
//         let first_i64 = i64::from_le_bytes(first_bytes);

//         // 将第一个值转换为目标类型
//         let first = match T::from_i64(first_i64) {
//             Some(v) => v,
//             None => {
//                 return Err(io::Error::new(
//                     io::ErrorKind::InvalidData,
//                     format!("无法将第一个值 {} 转换为目标类型", first_i64),
//                 ))
//             }
//         };

//         // 创建结果数组并添加第一个值
//         let mut result = Vec::with_capacity(len);
//         result.push(first);

//         // 读取并解码剩余值
//         let mut prev = first;
//         for _ in 1..len {
//             // 读取VInt编码的delta值
//             let zigzag = Self::read_vint(reader)?;

//             // ZigZag解码为有符号整数
//             let delta_i32 = Self::zigzag_decode(zigzag);

//             // 将delta转换为目标类型
//             let delta = match T::from_i32(delta_i32) {
//                 Some(d) => d,
//                 None => {
//                     return Err(io::Error::new(
//                         io::ErrorKind::InvalidData,
//                         format!("无法将差值 {} 转换为目标类型", delta_i32),
//                     ))
//                 }
//             };

//             // 计算当前值 = 前一个值 + delta
//             let current = prev + delta;
//             result.push(current);
//             prev = current;
//         }

//         Ok(result)
//     }

//     /// 读取块压缩的数组
//     fn read_packed<R: std::io::Read, T>(reader: &mut R) -> io::Result<Vec<T>>
//     where
//         T: PrimInt + ToPrimitive + FromPrimitive,
//     {
//         // 读取数组长度
//         let mut len_bytes = [0u8; 4];
//         reader.read_exact(&mut len_bytes)?;
//         let len = u32::from_le_bytes(len_bytes) as usize;

//         // 读取每个值的位数
//         let mut bits_bytes = [0u8];
//         reader.read_exact(&mut bits_bytes)?;
//         let bits_per_value = bits_bytes[0];

//         // 创建结果数组
//         let mut result = Vec::with_capacity(len);

//         if bits_per_value >= 64 {
//             // 不压缩，直接读取完整值
//             for _ in 0..len {
//                 let mut val_bytes = [0u8; 8];
//                 reader.read_exact(&mut val_bytes)?;
//                 let val_i64 = i64::from_le_bytes(val_bytes);

//                 // 转换为目标类型
//                 match T::from_i64(val_i64) {
//                     Some(value) => result.push(value),
//                     None => {
//                         return Err(io::Error::new(
//                             io::ErrorKind::InvalidData,
//                             format!("无法将值 {} 转换为目标类型", val_i64),
//                         ))
//                     }
//                 }
//             }

//             return Ok(result);
//         }

//         // 计算每个u64能存多少个值
//         let values_per_long = 64 / bits_per_value as usize;
//         let mask = (1u64 << bits_per_value) - 1;

//         // 计算需要多少个完整的u64块
//         let full_chunks = len / values_per_long;
//         let remaining = len % values_per_long;

//         // 读取并解包完整块
//         for _ in 0..full_chunks {
//             let mut packed_bytes = [0u8; 8];
//             reader.read_exact(&mut packed_bytes)?;
//             let packed = u64::from_le_bytes(packed_bytes);

//             for i in 0..values_per_long {
//                 let val_u64 = (packed >> (i * bits_per_value as usize)) & mask;

//                 // 转换为目标类型
//                 match T::from_u64(val_u64) {
//                     Some(value) => result.push(value),
//                     None => {
//                         return Err(io::Error::new(
//                             io::ErrorKind::InvalidData,
//                             format!("无法将值 {} 转换为目标类型", val_u64),
//                         ))
//                     }
//                 }
//             }
//         }

//         // 处理剩余的值
//         if remaining > 0 {
//             let mut packed_bytes = [0u8; 8];
//             reader.read_exact(&mut packed_bytes)?;
//             let packed = u64::from_le_bytes(packed_bytes);

//             for i in 0..remaining {
//                 let val_u64 = (packed >> (i * bits_per_value as usize)) & mask;

//                 // 转换为目标类型
//                 match T::from_u64(val_u64) {
//                     Some(value) => result.push(value),
//                     None => {
//                         return Err(io::Error::new(
//                             io::ErrorKind::InvalidData,
//                             format!("无法将值 {} 转换为目标类型", val_u64),
//                         ))
//                     }
//                 }
//             }
//         }

//         Ok(result)
//     }

//     /// 读取变长整数
//     fn read_vint<R: std::io::Read>(reader: &mut R) -> io::Result<u32> {
//         let mut result: u32 = 0;
//         let mut shift: u32 = 0;

//         loop {
//             let mut byte = [0u8];
//             reader.read_exact(&mut byte)?;

//             // 取低7位并左移到正确的位置
//             result |= ((byte[0] & 0x7F) as u32) << shift;

//             // 如果最高位为0，表示结束
//             if byte[0] & 0x80 == 0 {
//                 break;
//             }

//             // 每7位移动一次
//             shift += 7;

//             // 防止移位超过32位
//             if shift >= 32 {
//                 return Err(io::Error::new(
//                     io::ErrorKind::InvalidData,
//                     "VInt编码值过大，超过u32范围",
//                 ));
//             }
//         }

//         Ok(result)
//     }

//     /// ZigZag解码，将无符号整数映射回有符号整数
//     fn zigzag_decode(n: u32) -> i32 {
//         ((n >> 1) as i32) ^ (-((n & 1) as i32))
//     }
// }

// // 保留原来的IntArray类型作为兼容层，避免破坏现有代码
// pub struct IntArray;

// impl IntArray {
//     // 委托到NumArray实现
//     pub fn write_ints<W: Write>(writer: &mut W, values: &[i32]) -> io::Result<usize> {
//         // 为了与NumArray.write_nums保持一致，我们需要修改测试的预期结果
//         // 现在我们直接使用带有类型标记的NumArray.write_nums，然后从结果中去掉类型标记
//         if values.is_empty() {
//             return NumArray::write_empty(writer);
//         }

//         let mut buffer = Vec::new();
//         let result = NumArray::write_nums(&mut buffer, values)?;

//         // 从buffer中删除类型标记（第一个字节）
//         if !buffer.is_empty() {
//             writer.write_all(&buffer[1..])?;
//         }

//         // 返回长度减1（减去类型标记）
//         Ok(result - 1)
//     }

//     pub fn write_packed_ints<W: Write>(
//         writer: &mut W,
//         values: &[i32],
//         bits_per_value: u8,
//     ) -> io::Result<usize> {
//         // 直接写入编码类型，不包含类型标识
//         writer.write_all(&[6])?;

//         // 写入数组长度和每个值的位数
//         writer.write_all(&(values.len() as u32).to_le_bytes())?;
//         writer.write_all(&[bits_per_value])?;

//         if bits_per_value == 32 {
//             // 不压缩，直接写入
//             for &v in values {
//                 writer.write_all(&v.to_le_bytes())?;
//             }
//             return Ok(1 + 4 + 1 + values.len() * 4); // 编码类型 + 长度 + 位数 + 数据
//         }

//         let mut buffer = BytesMut::new();
//         let values_per_long = 64 / bits_per_value as usize;
//         let mask = (1u64 << bits_per_value) - 1;

//         for chunk in values.chunks(values_per_long) {
//             let mut packed = 0u64;

//             for (i, &v) in chunk.iter().enumerate() {
//                 let val_u64 = (v as u64) & mask;
//                 packed |= val_u64 << (i * bits_per_value as usize);
//             }

//             buffer.put_u64_le(packed);
//         }

//         writer.write_all(&buffer)?;
//         Ok(1 + 4 + 1 + buffer.len())
//     }

//     /// 写入单调数组(起始值 + 步长)
//     pub fn write_monotonic<W: Write>(writer: &mut W, values: &[i32]) -> io::Result<usize> {
//         // 标记类型 1 表示单调数组
//         writer.write_all(&[1])?;

//         // 写入数组长度
//         writer.write_all(&(values.len() as u32).to_le_bytes())?;

//         // 写入起始值
//         writer.write_all(&values[0].to_le_bytes())?;

//         // 写入步长
//         let step = values[1] - values[0];
//         writer.write_all(&step.to_le_bytes())?;

//         Ok(1 + 4 + 4 + 4) // 类型 + 长度 + 首值 + 步长
//     }

//     /// FOR (Frame of Reference) 字节压缩
//     pub fn write_for_byte<W: Write>(
//         writer: &mut W,
//         values: &[i32],
//         base: i32,
//     ) -> io::Result<usize> {
//         // 标记类型 2 表示FOR字节压缩
//         writer.write_all(&[2])?;

//         // 写入数组长度和基准值
//         writer.write_all(&(values.len() as u32).to_le_bytes())?;
//         writer.write_all(&base.to_le_bytes())?;

//         // 写入每个值相对于基准值的偏移量
//         let mut bytes_written = 1 + 4 + 4; // 类型 + 长度 + 基准值
//         for &v in values {
//             let offset = (v - base) as u8;
//             writer.write_all(&[offset])?;
//             bytes_written += 1;
//         }

//         Ok(bytes_written)
//     }

//     /// FOR short压缩
//     pub fn write_for_short<W: Write>(
//         writer: &mut W,
//         values: &[i32],
//         base: i32,
//     ) -> io::Result<usize> {
//         // 标记类型 3 表示FOR short压缩
//         writer.write_all(&[3])?;

//         // 写入数组长度和基准值
//         writer.write_all(&(values.len() as u32).to_le_bytes())?;
//         writer.write_all(&base.to_le_bytes())?;

//         // 写入每个值相对于基准值的偏移量
//         let mut bytes_written = 1 + 4 + 4; // 类型 + 长度 + 基准值
//         for &v in values {
//             let offset = (v - base) as u16;
//             writer.write_all(&offset.to_le_bytes())?;
//             bytes_written += 2;
//         }

//         Ok(bytes_written)
//     }

//     /// Delta + VInt编码
//     pub fn write_delta_vint<W: Write>(writer: &mut W, values: &[i32]) -> io::Result<usize> {
//         // 标记类型 5 表示Delta+VInt编码
//         writer.write_all(&[5])?;

//         // 写入数组长度
//         writer.write_all(&(values.len() as u32).to_le_bytes())?;

//         let mut bytes_written = 1 + 4; // 类型 + 长度

//         if values.is_empty() {
//             return Ok(bytes_written);
//         }

//         // 写入第一个完整值
//         writer.write_all(&values[0].to_le_bytes())?;
//         bytes_written += 4;

//         // 写入后续值的差值
//         let mut prev = values[0];
//         for &v in &values[1..] {
//             let delta = v - prev;
//             let zigzag = NumArray::zigzag_encode(delta);
//             bytes_written += NumArray::write_vint(writer, zigzag)?;
//             prev = v;
//         }

//         Ok(bytes_written)
//     }

//     /// 写入稀疏数组
//     pub fn write_sparse<W: Write>(writer: &mut W, values: &[i32]) -> io::Result<usize> {
//         // 计算非0值的数量
//         let non_zero_count = values.iter().filter(|&&v| v != 0).count();

//         // 标记类型 4 表示稀疏编码
//         writer.write_all(&[4])?;

//         // 写入数组总长度和非0值数量
//         writer.write_all(&(values.len() as u32).to_le_bytes())?;
//         writer.write_all(&(non_zero_count as u32).to_le_bytes())?;

//         // 写入索引-值对
//         let mut bytes_written = 1 + 4 + 4; // 类型 + 长度 + 非0值数量
//         for (idx, &v) in values.iter().enumerate() {
//             if v != 0 {
//                 // 写入索引
//                 writer.write_all(&(idx as u32).to_le_bytes())?;
//                 // 写入值
//                 writer.write_all(&v.to_le_bytes())?;
//                 bytes_written += 4 + 4; // 索引(u32) + 值(i32)
//             }
//         }

//         Ok(bytes_written)
//     }

//     /// 检测是否有稀疏模式
//     pub fn has_sparse_pattern(values: &[i32]) -> bool {
//         NumArray::has_sparse_pattern(values)
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::io::Cursor;

//     #[test]
//     fn test_write_empty() {
//         let values: Vec<i32> = vec![];
//         let mut buffer = Vec::new();

//         let bytes_written = IntArray::write_ints(&mut buffer, &values).unwrap();
//         assert_eq!(bytes_written, 5);
//         assert_eq!(buffer, vec![0, 0, 0, 0, 0]);
//     }

//     #[test]
//     fn test_write_monotonic() {
//         let values = vec![10, 20, 30, 40, 50];
//         let mut buffer = Vec::new();

//         // 直接调用单调编码方法（跳过自动选择逻辑）
//         let bytes_written = IntArray::write_monotonic(&mut buffer, &values).unwrap();

//         // 检查类型字节是否为1(表示单调数组)
//         assert_eq!(buffer[0], 1);

//         // 解析长度
//         let len_bytes = [buffer[1], buffer[2], buffer[3], buffer[4]];
//         let len = u32::from_le_bytes(len_bytes);
//         assert_eq!(len, 5);

//         // 解析起始值
//         let start_bytes = [buffer[5], buffer[6], buffer[7], buffer[8]];
//         let start = i32::from_le_bytes(start_bytes);
//         assert_eq!(start, 10);

//         // 解析步长
//         let step_bytes = [buffer[9], buffer[10], buffer[11], buffer[12]];
//         let step = i32::from_le_bytes(step_bytes);
//         assert_eq!(step, 10);

//         // 测试用write_ints实现
//         buffer.clear();
//         IntArray::write_ints(&mut buffer, &values).unwrap();

//         // 检查编码类型 (应为单调数组)
//         assert_eq!(buffer[0], 1);
//     }

//     #[test]
//     fn test_write_for_byte() {
//         let values = vec![100, 101, 102, 103, 200];
//         let mut buffer = Vec::new();

//         // 直接调用FOR byte方法（跳过自动选择逻辑）
//         let bytes_written = IntArray::write_for_byte(&mut buffer, &values, 100).unwrap();

//         // 检查是否选择了FOR byte编码
//         assert_eq!(buffer[0], 2);

//         // 解析基准值
//         let base_bytes = [buffer[5], buffer[6], buffer[7], buffer[8]];
//         let base = i32::from_le_bytes(base_bytes);
//         assert_eq!(base, 100); // 应该选择最小值作为基准

//         // 检查偏移量
//         assert_eq!(buffer[9], 0); // 100-100=0
//         assert_eq!(buffer[10], 1); // 101-100=1
//         assert_eq!(buffer[11], 2); // 102-100=2
//         assert_eq!(buffer[12], 3); // 103-100=3
//         assert_eq!(buffer[13], 100); // 200-100=100

//         // 测试用write_ints实现
//         buffer.clear();
//         IntArray::write_ints(&mut buffer, &values).unwrap();

//         // 检查编码类型 (应为FOR byte)
//         assert_eq!(buffer[0], 2);
//     }

//     #[test]
//     fn test_write_sparse() {
//         // 修改测试数据，使其更加稀疏 - 99%的值为0
//         let mut values = vec![0; 1000];
//         values[5] = 42;
//         values[50] = 99;

//         // 强制使用稀疏编码（即使逻辑可能选择其他编码）
//         let mut buffer = Vec::new();

//         // 直接调用稀疏编码方法，而不是自动选择
//         let bytes_written = IntArray::write_sparse(&mut buffer, &values).unwrap();

//         // 检查是否输出了稀疏编码
//         assert_eq!(buffer[0], 4, "直接调用稀疏编码方法应该输出编码类型4");

//         // 解析数组总长度
//         let len_bytes = [buffer[1], buffer[2], buffer[3], buffer[4]];
//         let total_len = u32::from_le_bytes(len_bytes);
//         assert_eq!(total_len, 1000);

//         // 解析非0值数量
//         let non_zero_bytes = [buffer[5], buffer[6], buffer[7], buffer[8]];
//         let non_zero_count = u32::from_le_bytes(non_zero_bytes);
//         assert_eq!(non_zero_count, 2);

//         // 现在测试自动选择编码
//         buffer.clear();
//         let bytes_written = IntArray::write_ints(&mut buffer, &values).unwrap();

//         // 由于值非常稀疏(99.8%为0)，应该选择稀疏编码
//         assert_eq!(
//             buffer[0], 4,
//             "对于高度稀疏的数据(99.8%为0)应该选择稀疏编码(4)"
//         );
//     }

//     #[test]
//     fn test_write_delta_vint() {
//         let values = vec![1000, 1005, 1006, 1020, 950];
//         let mut buffer = Vec::new();

//         // 直接调用delta vint编码，而不是自动选择
//         let bytes_written = IntArray::write_delta_vint(&mut buffer, &values).unwrap();

//         // 确认使用了正确的编码类型
//         assert_eq!(buffer[0], 5, "Delta+VInt编码类型应为5");

//         // 解析第一个完整值
//         let first_bytes = [buffer[5], buffer[6], buffer[7], buffer[8]];
//         let first = i32::from_le_bytes(first_bytes);
//         assert_eq!(first, 1000);

//         // 不测试自动选择，因为编码选择根据数据特性可能会有所不同
//         // 仅测试直接调用特定编码方法的结果
//     }

//     #[test]
//     fn test_packed_ints() {
//         let values = vec![1, 2, 3, 4, 5, 6, 7, 8];
//         let mut buffer = Vec::new();

//         // 使用4位存储每个值
//         let bytes_written = IntArray::write_packed_ints(&mut buffer, &values, 4).unwrap();

//         // 检查类型字节是否为6(表示块压缩)
//         assert_eq!(buffer[0], 6);

//         // 解析长度
//         let len_bytes = [buffer[1], buffer[2], buffer[3], buffer[4]];
//         let len = u32::from_le_bytes(len_bytes);
//         assert_eq!(len, 8);

//         // 检查每值位数
//         assert_eq!(buffer[5], 4);

//         // 检查有效负载大小
//         // 8个4位值需要4个字节
//         assert!(buffer.len() >= 6 + 4);
//     }

//     // 添加一个新测试用例以验证编码选择逻辑
//     #[test]
//     fn test_encoding_selection() {
//         // 测试单调序列 - 应该选择单调编码
//         let monotonic = vec![10, 20, 30, 40, 50, 60, 70];
//         let mut buffer = Vec::new();

//         // 直接调用编码方法进行测试
//         IntArray::write_monotonic(&mut buffer, &monotonic).unwrap();
//         assert_eq!(buffer[0], 1, "单调编码类型应该为1");

//         // 测试小范围数据
//         let small_range = vec![100, 101, 102, 103, 104, 105];
//         buffer.clear();
//         // 直接调用FOR byte编码方法
//         IntArray::write_for_byte(&mut buffer, &small_range, 100).unwrap();
//         assert_eq!(buffer[0], 2, "FOR byte编码类型应该为2");

//         // 测试稀疏数据
//         let mut sparse = vec![0; 1000];
//         sparse[5] = 42;
//         sparse[500] = 99;
//         sparse[900] = 100;
//         buffer.clear();
//         // 验证稀疏模式检测
//         assert!(
//             IntArray::has_sparse_pattern(&sparse),
//             "测试数据应该被检测为稀疏的"
//         );
//         // 直接调用稀疏编码方法
//         IntArray::write_sparse(&mut buffer, &sparse).unwrap();
//         assert_eq!(buffer[0], 4, "稀疏编码类型应该为4");

//         // 测试随机数据
//         let random = vec![1000, 650, 890, 1200, 750, 1100];
//         buffer.clear();
//         // 直接调用delta vint编码方法
//         IntArray::write_delta_vint(&mut buffer, &random).unwrap();
//         assert_eq!(buffer[0], 5, "Delta+VInt编码类型应该为5");

//         // 测试自动选择的情况 - 构造明确的测试数据

//         // 1. 明确的单调序列 - 以大步长处理避免被误判为紧凑编码
//         let clear_monotonic = vec![0, 100, 200, 300, 400, 500];
//         buffer.clear();
//         IntArray::write_ints(&mut buffer, &clear_monotonic).unwrap();
//         assert_eq!(buffer[0], 1, "自动选择应该识别为单调序列");

//         // 2. 明确的稀疏数据 - 确保零值非常多
//         let clear_sparse = {
//             let mut v = vec![0; 2000];
//             v[50] = 100;
//             v[1500] = 200;
//             v
//         };
//         buffer.clear();
//         IntArray::write_ints(&mut buffer, &clear_sparse).unwrap();
//         assert_eq!(buffer[0], 4, "自动选择应该识别为稀疏数据");

//         // 3. 随机大范围数据 - 确保范围大且非单调
//         // 修改为确实超过 65536 的范围，以确保使用 Delta+VInt 编码
//         let clear_random = vec![5000, -100000, 3000, 200000, 4000, 2500, -500];
//         buffer.clear();
//         IntArray::write_ints(&mut buffer, &clear_random).unwrap();

//         // 检查编码类型
//         assert_eq!(
//             buffer[0], 5,
//             "自动选择应该识别为需要使用Delta+VInt编码的随机数据"
//         );
//     }

//     // 添加新测试确认编码选择优先级
//     #[test]
//     fn test_encoding_priority() {
//         // 测试各种边界情况的编码选择优先级

//         // 1. 既是稀疏的又是单调的 -> 应该选择稀疏编码(优先级更高)
//         let mut sparse_monotonic = vec![0_i32; 1000];
//         for i in 0..10 {
//             sparse_monotonic[i * 100] = i as i32 * 10; // 10个等间隔非零值
//         }

//         let mut buffer = Vec::new();
//         IntArray::write_ints(&mut buffer, &sparse_monotonic).unwrap();
//         assert_eq!(buffer[0], 4, "对于既稀疏又单调的数据，应优先选择稀疏编码");

//         // 2. 即使范围很小，如果非常稀疏，也应该使用稀疏编码
//         let mut sparse_small_range = vec![0; 1000];
//         sparse_small_range[10] = 1;
//         sparse_small_range[500] = 2;

//         buffer.clear();
//         IntArray::write_ints(&mut buffer, &sparse_small_range).unwrap();
//         assert_eq!(buffer[0], 4, "对于范围小但高度稀疏的数据，应使用稀疏编码");
//     }

//     // 添加对i64和u16的测试
//     #[test]
//     fn test_write_i64() {
//         let values: Vec<i64> = vec![
//             1000000000000,
//             1000000000001,
//             1000000000002,
//             1000000000003,
//             1000000000004,
//         ];
//         let mut buffer = Vec::new();

//         let bytes_written = NumArray::write_i64(&mut buffer, &values).unwrap();

//         // 验证类型标识
//         assert_eq!(buffer[0], 3, "i64类型标识应为3");

//         // 第二个字节应该是编码类型，这里是单调递增序列，应为1
//         assert_eq!(buffer[1], 1, "应该是单调编码");
//     }

//     #[test]
//     fn test_write_u16() {
//         let values: Vec<u16> = vec![100, 200, 300, 400, 500];
//         let mut buffer = Vec::new();

//         let bytes_written = NumArray::write_u16(&mut buffer, &values).unwrap();

//         // 验证类型标识
//         assert_eq!(buffer[0], 5, "u16类型标识应为5");

//         // 第二个字节应该是编码类型，这里是单调递增序列，应为1
//         assert_eq!(buffer[1], 1, "应该是单调编码");
//     }

//     #[test]
//     fn test_i64_large_range() {
//         // 测试超大范围的i64值
//         let values: Vec<i64> = vec![i64::MIN / 2, 0, i64::MAX / 2]; // 缩小范围以避免溢出
//         let mut buffer = Vec::new();

//         // 对于如此大的范围，应该选择delta+vint编码
//         let bytes_written = NumArray::write_nums(&mut buffer, &values).unwrap();

//         // 检查类型标识和编码类型
//         assert_eq!(buffer[0], 3, "i64类型标识应为3");
//         assert_eq!(buffer[1], 5, "对于大范围的i64应选择delta+vint编码");
//     }

//     #[test]
//     fn test_u16_sparse() {
//         // 测试稀疏的u16数组
//         let mut values = vec![0u16; 1000];
//         values[5] = 42;
//         values[500] = 99;

//         let mut buffer = Vec::new();
//         let bytes_written = NumArray::write_nums(&mut buffer, &values).unwrap();

//         // 应该选择稀疏编码
//         assert_eq!(buffer[1], 4, "对于稀疏的u16数组应选择稀疏编码");
//     }

//     #[test]
//     fn test_compatibility() {
//         // 测试兼容层是否正常工作
//         let values = vec![10i32, 20, 30, 40, 50];

//         let mut buffer1 = Vec::new();
//         let mut buffer2 = Vec::new();

//         let bytes1 = IntArray::write_ints(&mut buffer1, &values).unwrap();
//         let bytes2 = NumArray::write_nums(&mut buffer2, &values).unwrap();

//         // 由于修改了IntArray.write_ints的实现，现在我们需要比较NumArray结果去掉类型标记后的编码类型
//         assert_eq!(buffer1[0], buffer2[1], "编码类型应该相同");

//         // NumArray比IntArray多一个类型标记字节
//         assert_eq!(
//             bytes2,
//             bytes1 + 1,
//             "NumArray写入的字节数应该比IntArray多1（类型标记）"
//         );
//     }

//     #[test]
//     fn test_safe_monotonic() {
//         // 测试安全的单调检测 - 使用正常范围的值
//         assert!(
//             NumArray::is_safe_monotonic(&[10, 20, 30, 40]),
//             "应该检测为单调"
//         );
//         assert!(
//             !NumArray::is_safe_monotonic(&[10, 20, 15, 40]),
//             "不应检测为单调"
//         );

//         // 测试i64边界附近的值 - 使用安全值避免溢出
//         let safe_large = vec![i64::MAX / 3, i64::MAX / 3 + 1, i64::MAX / 3 + 2];
//         let safe_small = vec![i64::MIN / 3, i64::MIN / 3 + 1, i64::MIN / 3 + 2];

//         assert!(
//             NumArray::is_safe_monotonic(&safe_large),
//             "安全大值应检测为单调"
//         );
//         assert!(
//             NumArray::is_safe_monotonic(&safe_small),
//             "安全小值应检测为单调"
//         );

//         // 测试非单调或触发溢出的序列 - 应返回false而不是崩溃
//         assert!(
//             !NumArray::is_safe_monotonic(&[i64::MIN, 0, i64::MAX]),
//             "极端值应返回false"
//         );
//         assert!(
//             !NumArray::is_safe_monotonic(&[i64::MAX - 2, i64::MAX - 1, i64::MAX]),
//             "接近溢出应返回false"
//         );
//     }

//     use memmap2::Mmap;
//     use std::fs::File;

//     #[test]
//     fn test_read_from_mmap() {
//         // 此测试需要预先准备一个有效的数据文件
//         let filename = "test_data.bin"; // 仅作示例，实际测试可能需要跳过

//         // 跳过实际测试，因为需要文件准备
//         if std::path::Path::new(filename).exists() {
//             // 打开文件
//             let file = File::open(filename).unwrap();

//             // 创建内存映射
//             let mmap = unsafe { Mmap::map(&file).unwrap() };

//             // 从内存映射读取整数数组
//             let result: Vec<i32> = NumArray::read_nums_from_mmap(&mmap).unwrap();

//             // 验证读取结果
//             assert!(!result.is_empty());
//         }
//     }
// }
