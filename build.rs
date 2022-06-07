extern crate cc;
use std::env;
use std::path::Path;
use std::process::Command;

fn main() {
    match env::var("MOCK_DEC_NUMBER") {
        Ok(val) => {
            if val == "1" {
                // mock decNumber
            } else {
                return;
            }
        }
        _ => return,
    }

    // Mock Tarantool decNumber library for a `cargo test` build.
    let litend = if cfg!(target_endian = "little") {
        "1"
    } else {
        "0"
    };

    // ../sbroad/target/debug/build/sbroad-703f9e463350cae9/build-script-build
    let current_file = std::env::current_exe().unwrap();
    let root: &Path = current_file
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let tarantool = root.join("deps").join("tarantool");
    let dec_number = tarantool.join("third_party").join("decNumber");
    let msg_puck = tarantool
        .join("deps")
        .join("tarantool")
        .join("src")
        .join("lib")
        .join("msgpuck");

    // We need to configure Tarantool with cmake in order to generate all
    // required headers for the further build of the decNumber mocking
    // library (used in the unit tests).
    //
    // Mostly the problem is with `decimal.c` that depends on the `small`
    // allocator requiring `trivia/config.h`.
    Command::new("cmake")
        .current_dir(tarantool.clone())
        .arg(".")
        .arg("-DCMAKE_BUILD_TYPE=Release")
        .status()
        .expect("failed to run cmake");
    cc::Build::new()
        .warnings(false)
        .include(dec_number.to_str().unwrap())
        .include(msg_puck.to_str().unwrap())
        .include(tarantool.join("third_party").to_str().unwrap())
        .include(tarantool.join("src").to_str().unwrap())
        .include(
            tarantool
                .join("src")
                .join("lib")
                .join("small")
                .join("include"),
        )
        .include(
            tarantool
                .join("src")
                .join("lib")
                .join("small")
                .join("third_party"),
        )
        .file(dec_number.join("decContext.c").to_str().unwrap())
        .file(dec_number.join("decNumber.c").to_str().unwrap())
        .file(dec_number.join("decPacked.c").to_str().unwrap())
        .file(
            tarantool
                .join("src")
                .join("lib")
                .join("core")
                .join("decimal.c")
                .to_str()
                .unwrap(),
        )
        .define("DECLITEND", Some(litend))
        // do not inline functions in msgpuck.h for a static library
        .define("MP_LIBRARY", None)
        .compile("libdecNumber.a");
}
