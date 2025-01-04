use std::fs;

use prost_build::Config;

const PROTO_DIR: &str = "./proto";

fn main() {
    let proto_files = fs::read_dir(PROTO_DIR)
        .unwrap()
        .map(|f| f.unwrap().path().to_str().unwrap().to_string())
        .collect::<Vec<String>>();

    println!("cargo:warning=building protobuf files: {:?}", proto_files);

    for proto_file in proto_files.iter() {
        println!("cargo:rerun-if-changed={}", proto_file);
    }

    if !proto_files.is_empty() {
        let mut config = Config::new();

        match config
            .type_attribute("PicachvMessages.BinaryOperator", "#[derive(Hash)]")
            .type_attribute("PicachvMessages.BinaryOperator.operator", "#[derive(Hash)]")
            .out_dir("./src")
            .compile_protos(&proto_files, &["proto"])
        {
            Ok(_) => (),
            Err(e) => panic!("Failed to compile protos: {}", e),
        }
    }
}
