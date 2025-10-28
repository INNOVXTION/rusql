mod pager;
mod errors;


use std::error::Error;
use std::ffi::OsStr;
use std::fs::{rename, DirBuilder, File};
use std::io::{Write};
use std::path::{Path, PathBuf};
use std::{env, io};
use tracing_subscriber;
use tracing::{event, info, instrument, Level};

fn main() -> Result<(), Box<dyn Error>> {
    let env = env::var("INFO_LEVEL")
        .map_or("rusql=error".to_string(), |v| "rusql=".to_owned() + &v);
    tracing_subscriber::fmt()
        .with_env_filter(env)
        .init(); 

    let s = String::from("Hello World");
    let p = Path::new(r"files\bar.txt");
    let res = write_file(&s)?;
    let res2 = write_file_tmp(&s, p);

    Ok(())
}

#[instrument]
fn write_file(data: &str) -> io::Result<()> {
    event!(target: "rusql", Level::INFO, "writing file");
    let mut new_file = File::create("foo.txt")?;
    new_file.write_all(data.as_bytes())?;
    Ok(())
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