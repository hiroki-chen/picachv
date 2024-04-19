use std::fs;

use prost_build::Config;

const PROTO_DIR: &str = "./proto";

fn main() {
    let proto_files = fs::read_dir(PROTO_DIR)
        .unwrap()
        .map(|f| f.unwrap().path().to_str().unwrap().to_string())
        .collect::<Vec<String>>();

    println!("cargo:warning={:?}", proto_files);

    if !proto_files.is_empty() {
        let mut config = Config::new();
        config.out_dir("./src");
        match config.compile_protos(&proto_files, &["proto"]) {
            Ok(_) => (),
            Err(e) => panic!("Failed to compile protos: {}", e.to_string()),
        }
    }
}
