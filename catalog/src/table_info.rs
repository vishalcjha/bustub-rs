#![allow(dead_code)]

use crate::{schema::Schema, TableOid};

pub struct TableInfo {
    schema: Schema,
    name: String,
    // TODO add link to table heap.
    table_oid: TableOid,
}
