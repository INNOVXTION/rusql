mod pager;
mod errors;
mod helper;

use std::error::Error;
use std::fs::{DirBuilder, File, OpenOptions, rename};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::{env, io};
use tracing_subscriber;
use tracing::{Level, event, info, instrument};
use std::convert::TryInto;

const BLOCK_SIZE: usize = 50;

#[derive(Debug)]
struct Row {
    id: u32,
    name: [u8; 20],
    age: u32,
}

fn main() -> Result<(), Box<dyn Error>> {
    let env = env::var("INFO_LEVEL")
        .map_or("rusql=info".to_string(), |v| "rusql=".to_owned() + &v);
    tracing_subscriber::fmt()
        .with_env_filter(env)
        .init(); 

    let p1 = Row {
        id: 1,
        name: *encode_str(String::from("Alice")),
        age: 20
    };
    let p2 = Row {
        id: 2,
        name: *encode_str(String::from("Bob")),
        age: 20
    };
    encode(p1, 0)?;
    decode()?;
    Ok(())
}


#[instrument]
fn encode(data: Row, page: u64) -> io::Result<()> {
    event!(target: "rusql", Level::INFO, "encoding file");
    let mut db = OpenOptions::new().write(true).open("database.rdb")?;
    let mut buf: Vec<u8> = Vec::with_capacity(BLOCK_SIZE);
    buf.extend_from_slice(&data.id.to_le_bytes());
    buf.extend_from_slice(&data.name);
    buf.extend_from_slice(&data.age.to_le_bytes());
    db.write_all(&buf)?;
    Ok(())
}

#[instrument]
fn decode() -> io::Result<()> {
    let mut file = File::open("database.rdb")?;
    let mut buf = [0u8; BLOCK_SIZE];
    buf.as_slice();
    file.read(&mut buf)?;
    event!(target: "rusql", Level::INFO, "decoding file {:?}", buf);
    let person = Row {
        id: u32::from_le_bytes(buf[..4].try_into().unwrap()),
        name: { 
            let mut arr = [0u8;20];
            arr.copy_from_slice(&buf[4..24]);
            arr
        },
        age: u32::from_le_bytes(buf[24..28].try_into().unwrap()),

    };
    let name_len: usize = usize::from_le_bytes(person.name[..8].try_into().unwrap());
    let name = String::from_utf8(person.name[8..8 + name_len].to_vec()).unwrap();
    info!("name: {:?}, id: {}, age: {}", name, person.id, person.age);
    Ok(())
}


#[instrument]
fn encode_str(string: String) -> Box<[u8; 20]> {
    let len = string.len().to_le_bytes();
    let data = string.as_bytes();
    event!(target: "rusql", Level::INFO, "len: {:?}, data: {:?}", len, data);
    let mut buf = Vec::with_capacity(BLOCK_SIZE);
    buf.extend_from_slice(&len);
    buf.extend_from_slice(data);
    if buf.len() < 20 {
        buf.resize(20, 0);
    }
    let new_box: Box<[u8; 20]> = buf.into_boxed_slice().try_into().unwrap();
    event!(target: "rusql", Level::INFO, "string encoded: {:?}", *new_box);
    new_box
}


#[instrument]
fn write_file_tmp(data: &str, path: &Path) -> io::Result<()> {
    info!("writing atomically!");
    let mut tmp_path= PathBuf::from(path.parent().unwrap());
    if !tmp_path.is_dir() {
        info!("directoy not found at, {:?}, creating new directory", &tmp_path);
        DirBuilder::new()
            .recursive(true)
            .create(&tmp_path)?;
    }
    let mut tmp_filename = path.file_stem().unwrap().to_os_string();
    tmp_filename.push("_tmp.");
    tmp_filename.push(path.extension().unwrap());
    tmp_path.push(tmp_filename);
    
    let mut new_file = File::create(&tmp_path)?;
    new_file.write_all(data.as_bytes())?;
    rename(&tmp_path, path)?;
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {

    }
}