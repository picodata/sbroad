use std::path::PathBuf;

#[allow(dead_code)]
struct Location {
    current: PathBuf,
    tarantool: PathBuf,
    root: PathBuf,
    dec_number: PathBuf,
    msg_puck: PathBuf,
}

#[allow(clippy::too_many_lines)]
fn main() {
    #[cfg(feature = "mock")]
    {
        use std::process::Command;

        let location = get_path();

        // We need to configure Tarantool with cmake in order to generate all
        // required headers for the further build of the decNumber mocking
        // library (used in the unit tests).
        //
        // Mostly the problem is with `decimal.c` that depends on the `small`
        // allocator requiring `trivia/config.h`.
        Command::new("cmake")
            .current_dir(location.tarantool.clone())
            .arg(".")
            .arg("-DCMAKE_BUILD_TYPE=Release")
            .status()
            .expect("failed to run cmake");

        // Build message pack shared library.
        let common_args = ["-O0", "-g", "-fno-omit-frame-pointer", "-fPIC", "-std=c99"];
        let inline_args = ["-DMP_PROTO=inline", "-DMP_IMPL=inline"];
        // hints.o
        Command::new("cc")
            .current_dir(location.tarantool.clone())
            .args(common_args)
            .args(inline_args)
            .args(["-I", &format!("{}", location.msg_puck.display())])
            .args([
                "-o",
                &format!("{}/hints.o", location.current.display()).as_str(),
            ])
            .args([
                "-c",
                &format!("{}/hints.c", location.msg_puck.display()).as_str(),
            ])
            .status()
            .expect("failed to compile hints.c");
        // msgpuck.o
        Command::new("cc")
            .current_dir(location.tarantool.clone())
            .args(common_args)
            .args(inline_args)
            .args(["-I", &format!("{}", location.msg_puck.display()).as_str()])
            .args([
                "-o",
                &format!("{}/msgpuck.o", location.current.display()).as_str(),
            ])
            .args([
                "-c",
                &format!("{}/msgpuck.c", location.msg_puck.display()).as_str(),
            ])
            .status()
            .expect("failed to compile msgpuck.c");
        // static.o
        Command::new("cc")
            .current_dir(location.tarantool.clone())
            .args(common_args)
            .args(inline_args)
            .args([
                "-I",
                &format!(
                    "{}",
                    location
                        .tarantool
                        .join("src")
                        .join("lib")
                        .join("small")
                        .join("third_party")
                        .display()
                )
                .as_str(),
            ])
            .args([
                "-I",
                &format!(
                    "{}",
                    location
                        .tarantool
                        .join("src")
                        .join("lib")
                        .join("small")
                        .join("include")
                        .join("small")
                        .display()
                )
                .as_str(),
            ])
            .args([
                "-o",
                format!("{}/static.o", location.current.display()).as_str(),
            ])
            .args([
                "-c",
                format!(
                    "{}/static.c",
                    location
                        .tarantool
                        .join("src")
                        .join("lib")
                        .join("small")
                        .join("small")
                        .display()
                )
                .as_str(),
            ])
            .status()
            .expect("failed to compile static.c");
        // link all the objects together
        Command::new("cc")
            .current_dir(location.tarantool.clone())
            .arg("-shared")
            .args([
                "-o",
                format!("{}/libmsgpuck.so", location.root.join("target").display()).as_str(),
            ])
            .arg(format!("{}/static.o", location.current.display()).as_str())
            .arg(format!("{}/msgpuck.o", location.current.display()).as_str())
            .arg(format!("{}/hints.o", location.current.display()).as_str())
            .status()
            .expect("failed to build libmsgpuck.so");

        // Build decNumber shared library.
        compile_dec_object("decContext", &location.dec_number, &location);
        compile_dec_object("decNumber", &location.dec_number, &location);
        compile_dec_object("decPacked", &location.dec_number, &location);
        compile_dec_object(
            "decimal",
            &location.tarantool.join("src").join("lib").join("core"),
            &location,
        );
        Command::new("cc")
            .current_dir(location.tarantool.clone())
            .arg("-shared")
            .args([
                "-o",
                format!("{}/libdecNumber.so", location.root.join("target").display()).as_str(),
            ])
            .arg(format!("{}/decContext.o", location.current.display()).as_str())
            .arg(format!("{}/decNumber.o", location.current.display()).as_str())
            .arg(format!("{}/decPacked.o", location.current.display()).as_str())
            .arg(format!("{}/decimal.o", location.current.display()).as_str())
            .args(["-l", "msgpuck"])
            .args([
                "-L",
                &format!("{}", location.root.join("target").display()).as_str(),
            ])
            .status()
            .expect("failed to build libdecNumber.so");

        println!(
            "cargo:rustc-link-search=native={}",
            location.root.join("target").display()
        );
        println!("cargo:rustc-link-lib=dylib=decNumber");
    }
}

#[cfg(feature = "mock")]
fn get_path() -> Location {
    use std::path::Path;

    // ../sbroad/target/debug/build/sbroad-703f9e463350cae9/build-script-build
    let current_file = std::env::current_exe().unwrap();
    let current_dir = current_file.parent().unwrap();
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
    let msg_puck = tarantool.join("src").join("lib").join("msgpuck");
    Location {
        current: current_dir.to_path_buf(),
        tarantool: tarantool.to_path_buf(),
        root: root.to_path_buf(),
        dec_number: dec_number.to_path_buf(),
        msg_puck: msg_puck.to_path_buf(),
    }
}

#[cfg(feature = "mock")]
fn compile_dec_object(obj_name: &str, obj_dir: &PathBuf, location: &Location) {
    use std::process::Command;

    // Mock Tarantool decNumber library for a `cargo test` build.
    let litend = if cfg!(target_endian = "little") {
        "1"
    } else {
        "0"
    };

    let common_args = ["-O0", "-g", "-fno-omit-frame-pointer", "-fPIC"];
    let dec_args = [
        format!("-DDECLITEND={}", litend),
        "-DMP_LIBRARY".to_string(),
    ];

    Command::new("cc")
        .current_dir(location.dec_number.clone())
        .args(common_args)
        .args(dec_args)
        .args(["-I", format!("{}", location.dec_number.display()).as_str()])
        .args(["-I", format!("{}", location.msg_puck.display()).as_str()])
        .args([
            "-I",
            format!("{}", location.tarantool.join("third_party").display()).as_str(),
        ])
        .args([
            "-I",
            format!("{}", location.tarantool.join("src").display()).as_str(),
        ])
        .args([
            "-I",
            format!(
                "{}",
                location
                    .tarantool
                    .join("src")
                    .join("lib")
                    .join("small")
                    .join("include")
                    .display()
            )
            .as_str(),
        ])
        .args([
            "-I",
            format!(
                "{}",
                location
                    .tarantool
                    .join("src")
                    .join("lib")
                    .join("small")
                    .join("third_party")
                    .display()
            )
            .as_str(),
        ])
        .args([
            "-o",
            format!("{}/{}.o", location.current.display(), obj_name).as_str(),
        ])
        .args([
            "-c",
            format!(
                "{}/{}.c",
                &format!("{}", obj_dir.display()).as_str(),
                obj_name
            )
            .as_str(),
        ])
        .status()
        .expect(format!("failed to compile {}.c", obj_name).as_str());
}
