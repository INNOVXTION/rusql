use std::{marker::PhantomData, ops::Deref};

use crate::database::{
    api::tx::TX,
    errors::{Error, Result, TableError},
    pager::KVEngine,
    tables::{Key, Value},
    types::{BTREE_MAX_VAL_SIZE, DataCell},
};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, instrument};

/*
 * Encoding Layout:
 * |--------------KEY---------------|----Value-----|
 * |                  [Col1][Col2]..|[Col3][Col4]..|
 * |[TABLE ID][PREFIX][PK1 ][PK2 ]..|[ v1 ][ v2 ]..|
 *
 * Key: 0 Val: Tdef Schema
 *
 * Tdef, id = 1:
 * |-----KEY------|----Val---|
 * |      [ Col1 ]|[  Col2  ]|
 * |[1][0][ name ]|[  def   ]|
 *
 * Meta, id = 2:
 * |-----KEY------|----Val---|
 * |      [ Col1 ]|[  Col2  ]|
 * |[2][0][ key  ]|[  val   ]|
 *
 * Data Path:
 * User Input -> DataCell -> Record -> Key, Value -> Tree
 */

pub const DEF_TABLE_NAME: &'static str = "tdef";
pub const DEF_TABLE_COL1: &'static str = "name";
pub const DEF_TABLE_COL2: &'static str = "def";

pub const DEF_TABLE_ID: u32 = 1;
pub const DEF_TABLE_PKEYS: u16 = 1;

pub const META_TABLE_NAME: &'static str = "tmeta";
pub const META_TABLE_COL1: &'static str = "name";
pub const META_TABLE_COL2: &'static str = "tid";
pub const META_TABLE_ID_ROW: &'static str = "tid";

pub const META_TABLE_ID: u32 = 2;
pub const META_TABLE_PKEYS: u16 = 1;

pub const LOWEST_PREMISSIABLE_TID: u32 = DEF_TABLE_ID + META_TABLE_ID;
pub const PKEY_PREFIX: u16 = 0;

/// wrapper for sentinal value
#[derive(Serialize, Deserialize)]
pub(crate) struct MetaTable(Table);

impl MetaTable {
    pub fn new() -> Self {
        MetaTable(Table {
            name: META_TABLE_NAME.to_string(),
            id: META_TABLE_ID,
            cols: vec![
                Column {
                    title: META_TABLE_COL1.to_string(),
                    data_type: TypeCol::BYTES,
                },
                Column {
                    title: META_TABLE_COL2.to_string(),
                    data_type: TypeCol::INTEGER,
                },
            ],
            pkeys: META_TABLE_PKEYS,
            indices: vec![Index {
                name: META_TABLE_COL1.to_string(),
                columns: (0..META_TABLE_PKEYS as usize).collect(),
                prefix: PKEY_PREFIX,
                kind: IdxKind::Primary,
            }],
            _priv: PhantomData,
        })
    }

    pub fn as_table(self) -> Table {
        self.0
    }
}

/// wrapper for sentinal value
#[derive(Serialize, Deserialize)]
pub(crate) struct TDefTable(Table);

impl TDefTable {
    pub fn new() -> Self {
        TDefTable(Table {
            name: DEF_TABLE_NAME.to_string(),
            id: DEF_TABLE_ID,
            cols: vec![
                Column {
                    title: DEF_TABLE_COL1.to_string(),
                    data_type: TypeCol::BYTES,
                },
                Column {
                    title: DEF_TABLE_COL2.to_string(),
                    data_type: TypeCol::BYTES,
                },
            ],
            pkeys: DEF_TABLE_PKEYS,
            indices: vec![Index {
                name: DEF_TABLE_COL1.to_string(),
                columns: (0..DEF_TABLE_PKEYS as usize).collect(),
                prefix: PKEY_PREFIX,
                kind: IdxKind::Primary,
            }],
            _priv: PhantomData,
        })
    }
}

impl Deref for TDefTable {
    type Target = Table;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(crate) struct TableBuilder {
    name: Option<String>,
    id: Option<u32>,
    cols: Vec<Column>,
    pkeys: Option<u16>,
}

impl TableBuilder {
    pub fn new() -> Self {
        TableBuilder {
            name: None,
            id: None,
            cols: vec![],
            pkeys: None,
        }
    }

    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// deprecated, for testing purposes only
    ///
    /// build() requests a free TID when omitted
    pub fn id(mut self, id: u32) -> Self {
        self.id = Some(id);
        self
    }

    /// adds a column to the table, order sensitive
    pub fn add_col(mut self, title: &str, data_type: TypeCol) -> Self {
        self.cols.push(Column {
            title: title.to_string(),
            data_type,
        });
        self
    }

    /// declares amount of columns as primary keys, starting in order in which columns were added
    pub fn pkey(mut self, nkeys: u16) -> Self {
        self.pkeys = Some(nkeys);
        self
    }

    /// parsing is WIP
    pub fn build(self, pager: &mut TX) -> Result<Table> {
        let name = match self.name {
            Some(n) => {
                if n.is_empty() {
                    error!("table creation error");
                    Err(TableError::TableBuildError(
                        "must provide a name".to_string(),
                    ))?;
                }
                n
            }
            None => {
                error!("table creation error");
                return Err(TableError::TableBuildError(
                    "must provide a name".to_string(),
                ))?;
            }
        };

        let id = match self.id {
            Some(id) => id,
            None => pager.new_tid()?,
        };

        let cols = self.cols;
        if cols.len() < 1 {
            error!("table creation error");
            return Err(TableError::TableBuildError(
                "must provide at least 2 columns".to_string(),
            ))?;
        }

        let pkeys = match self.pkeys {
            Some(pk) => {
                if pk == 0 {
                    error!("table creation error");
                    return Err(TableError::TableBuildError(
                        "primary key cant be zero".to_string(),
                    ))?;
                }
                if cols.len() < pk as usize {
                    error!("table creation error");
                    return Err(TableError::TableBuildError(
                        "cant have more primary keys than columns".to_string(),
                    ))?;
                }
                pk
            }
            None => {
                error!("table creation error");
                return Err(TableError::TableBuildError(
                    "must designate primary keys".to_string(),
                ))?;
            }
        };

        let primary_idx = Index {
            name: cols[0].title.clone(),
            columns: (0..pkeys as usize).collect(),
            prefix: PKEY_PREFIX,
            kind: IdxKind::Primary,
        };

        Ok(Table {
            name,
            id,
            cols,
            pkeys,
            indices: vec![primary_idx],
            _priv: PhantomData,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(crate) struct Table {
    pub name: String,
    pub id: u32,
    pub cols: Vec<Column>,
    pub pkeys: u16,
    pub indices: Vec<Index>,

    // ensures tables are built through constructor
    _priv: PhantomData<bool>,
}

impl Table {
    /// encodes table to JSON string
    pub fn encode(&self) -> Result<String> {
        let data = serde_json::to_string(&self).map_err(|e| {
            error!(%e, "error when encoding table");
            TableError::SerializeTableError(e)
        })?;
        if data.len() > BTREE_MAX_VAL_SIZE {
            return Err(TableError::EncodeTableError(
                "Table schema exceeds value size limit".to_string(),
            )
            .into());
        }
        Ok(data)
    }

    /// decodes JSON string into table
    pub fn decode(value: Value) -> Result<Self> {
        serde_json::from_str(&value.to_string()).map_err(|e| {
            error!(%e, "error when decoding table");
            TableError::SerializeTableError(e).into()
        })
    }

    /// checks if col by name exists and matches data type
    pub fn valid_col(&self, title: &str, data: &DataCell) -> bool {
        let cell_type = match data {
            DataCell::Str(_) => TypeCol::BYTES,
            DataCell::Int(_) => TypeCol::INTEGER,
        };

        for col in self.cols.iter() {
            if col.title == title && col.data_type == cell_type {
                return true;
            }
        }
        false
    }

    fn col_exists(&self, title: &str) -> Option<u16> {
        for (i, col) in self.cols.iter().enumerate() {
            if col.title == title {
                return Some(i as u16);
            }
        }
        None
    }

    fn idx_exists(&self, title: &str) -> Option<usize> {
        for (i, idx) in self.indices.iter().enumerate() {
            if idx.name == title {
                return Some(i);
            }
        }
        None
    }

    /// adds a secondary index
    pub fn add_index(&mut self, col: &str) -> Result<()> {
        // check if column exists
        let col_idx = match self.col_exists(col) {
            Some(i) => i,
            None => {
                return Err(TableError::IndexCreateError("column doesnt exist".to_string()).into());
            }
        };

        // check for duplicate indices
        if self.idx_exists(col).is_some() {
            return Err(TableError::IndexCreateError("index already exists".to_string()).into());
        }

        self.indices.push(Index {
            name: col.to_string(),
            columns: vec![col_idx as usize],
            prefix: self.indices.len() as u16,
            kind: IdxKind::Secondary,
        });

        Ok(())
    }

    /// removes a secondary index
    pub fn remove_index(&mut self, name: &str) -> Result<()> {
        let idx = match self.idx_exists(name) {
            Some(i) => i,
            None => {
                return Err(TableError::IndexDeleteError("index doesnt exist!".to_string()).into());
            }
        };
        if self.indices[idx].kind == IdxKind::Primary {
            return Err(
                TableError::IndexDeleteError("cant delete primary index".to_string()).into(),
            );
        }
        self.indices.remove(idx);
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(crate) struct Index {
    /// unique identifier
    pub name: String,
    /// indices into the table.cols
    pub columns: Vec<usize>,
    /// prefix for encoding
    pub prefix: u16,
    pub kind: IdxKind,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(crate) enum IdxKind {
    Primary,
    Secondary,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(crate) struct Column {
    pub title: String,
    pub data_type: TypeCol,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(crate) enum TypeCol {
    BYTES = 1,
    INTEGER = 2,
}

impl TypeCol {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(TypeCol::BYTES),
            2 => Some(TypeCol::INTEGER),
            _ => None,
        }
    }
}
