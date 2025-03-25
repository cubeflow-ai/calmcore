use std::process::Child;

use crate::persist::num_ser::{i64_coder, u16_coder};

use super::*;

struct Lazy {
    items: Vec<u8>,
    node_length: Vec<u16>,
    offset_values: Vec<i64>,
}

pub struct TreeWriter<K, V> {
    tree: BTree<K, V>,
    key_len: u16,
    var_len: bool,
    serializer: Box<dyn KVSerializer<K, V>>,
}

impl<K, V> TreeWriter<K, V>
where
    K: Ord,
{
    pub fn new(tree: BTree<K, V>, key_len: u16, serializer: Box<dyn KVSerializer<K, V>>) -> Self {
        Self {
            tree,
            key_len,
            var_len: key_len == 0,
            serializer,
        }
    }
}

/// next_items_offset < 0 point to data file
///  MAGIC_VERSION:[u8;2] + key_len:u16 + node_count: u32
/// fixedkey_node
///         [item_count:u16 + key:[u8; key_len] + next_items_offset:[var(i64)]]
/// varkey_node
///         [item_count:u16 + key_len:var(u32) + key:[u8; key_len] + next_items_offset:[var(i64)]]
/// data
///    [
///         value_len:u32 + value:[u8; value_len]
///    ]
/// if key_len == 0 means not fixed key
impl<K, V> TreeWriter<K, V>
where
    K: Ord,
{
    pub fn persist(&self, dir: &PathBuf) -> Result<()> {
        if !dir.exists() {
            std::fs::create_dir_all(dir)?;
        }

        let mut node_file = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(dir.join(NODE_NAME))?,
        );

        let mut data_file = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(dir.join(DATA_NAME))?,
        );

        data_file.write_all(MAGIC_VERSION)?;

        node_file.write_all(MAGIC_VERSION)?;

        node_file.write_all(&self.key_len.to_be_bytes())?;
        node_file.write_all(&(self.tree.len() as u32).to_be_bytes())?;

        let root_offset = self.inner_persist(&self.tree.root, &mut node_file, &mut data_file)?;

        //write root offset
        node_file.write_all(root_offset.to_be_bytes().as_slice())?;

        node_file.flush()?;
        data_file.flush()?;

        Ok(())
    }

    fn inner_persist(
        &self,
        bt: &BTreeType<K, V>,
        node_file: &mut BufWriter<File>,
        data_file: &mut BufWriter<File>,
    ) -> Result<i64> {
        match bt {
            crate::BTreeType::Leaf(leaf) => self.persist_leaf(node_file, data_file, leaf),
            crate::BTreeType::Node(node) => self.persist_node(node_file, data_file, node),
        }
    }

    fn persist_leaf(
        &self,
        node_file: &mut BufWriter<File>,
        data_file: &mut BufWriter<File>,
        leaf: &Leaf<K, V>,
    ) -> Result<i64> {
        let items = &leaf.items;

        let start = node_file.stream_position()? as i64;

        let mut data_offsets = Vec::with_capacity(items.len());
        let mut node_data = Vec::new();
        let mut node_length: Vec<u16> = Vec::with_capacity(items.len());

        for k in items {
            let key_bytes = self.serializer.serialize_key(&k.0);
            if self.var_len {
                node_length.push(key_bytes.len() as u16);
            };
            node_data.extend_from_slice(&key_bytes);
            data_offsets.push(-(data_file.stream_position()? as i64));

            let value_bytes = self.serializer.serialize_value(&k.1);
            zigzag::write_u32(value_bytes.len() as u32, data_file)?;
            data_file.write_all(&value_bytes)?;
        }

        write_block(node_file, node_length, node_data, data_offsets)?;

        Ok(start)
    }

    fn persist_node(
        &self,
        node_file: &mut BufWriter<File>,
        data_file: &mut BufWriter<File>,
        node: &Node<K, V>,
    ) -> Result<i64> {
        let chidren = &node.children;

        let mut node_length = Vec::with_capacity(chidren.len());
        let mut data_offsets = Vec::with_capacity(chidren.len());
        let mut node_data = Vec::new();

        for t in chidren {
            if let Some(k) = t.key() {
                let key_bytes = self.serializer.serialize_key(&k.0);
                if self.var_len {
                    node_length.push(key_bytes.len() as u16);
                }
                node_data.extend_from_slice(&key_bytes);
            }
        }

        for c in chidren {
            data_offsets.push(self.inner_persist(c, node_file, data_file)?);
        }

        let start = node_file.stream_position()? as i64;

        write_block(node_file, node_length, node_data, data_offsets)?;

        Ok(start)
    }
}

fn write_block(
    node_file: &mut BufWriter<File>,
    node_length: Vec<u16>,
    node_data: Vec<u8>,
    data_offsets: Vec<i64>,
) -> Result<()> {
    let mut bytes = Vec::new();
    i64_coder::write(&mut bytes, &data_offsets)?;
    node_file.write_all(&bytes)?;

    bytes.clear();
    flate2::write::ZlibEncoder::new(&mut bytes, flate2::Compression::default())
        .write_all(&node_data)?;
    zigzag::write_u32(bytes.len() as u32, node_file)?;
    node_file.write_all(&bytes)?;

    bytes.clear();
    if !node_length.is_empty() {
        u16_coder::write(&mut bytes, &node_length)?;
        node_file.write_all(&bytes)?;
    }

    Ok(())
}
