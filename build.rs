use std::env;
use std::path::PathBuf;

fn main() {
    let dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let parser_path = PathBuf::from(dir).join("src").join("parser.c");

    cc::Build::new()
        .file(&parser_path)
        .include("src/tree_sitter")
        .compile("tree-sitter-sql");
}
