pub mod field;

pub struct Schema {
    pub name: String,
    pub primary_key: Option<String>,
    pub store_source: bool,
    pub fields: Vec<field::FieldOption>,
}
impl Schema {
    pub(crate) fn add_field(&mut self, field: field::FieldOption) {
        self.fields.push(field);
    }
}
