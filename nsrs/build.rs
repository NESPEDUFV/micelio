use std::path::PathBuf;

fn main() {
    let ns3_home = PathBuf::from(env!("NS3_HOME"));
    let ns3_include = {
        let mut ns3_include = ns3_home.clone();
        ns3_include.extend(["build", "include"]);
        ns3_include
    };
    cxx_build::bridge("src/lib.rs")
        .include(&ns3_include)
        .file("src/runtime.cc")
        .std("c++20")
        .compile("nsrs");

    println!("cargo:rerun-if-changed=src/runtime.cc");
    println!("cargo:rerun-if-changed=include/runtime.h");
}
