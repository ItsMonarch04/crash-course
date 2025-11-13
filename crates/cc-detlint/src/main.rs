// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]

use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

const FORBIDDEN: [(&str, &str); 12] = [
    ("std::time::SystemTime", "wall clock"),
    ("std::time::Instant", "host monotonic clock"),
    ("std::thread", "host scheduling"),
    ("tokio::", "async runtime scheduling"),
    ("async fn", "async runtime scheduling"),
    ("rand::", "ambient random source"),
    ("getrandom", "ambient random source"),
    ("thread_rng", "ambient random source"),
    ("HashMap", "randomized iteration order"),
    ("HashSet", "randomized iteration order"),
    ("f32", "floating-point behavior"),
    ("f64", "floating-point behavior"),
];

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("check") => check(&args[1..]),
        Some("double-run") => double_run(&args[1..]),
        _ => {
            println!(
                "cc-detlint {}\n\nCommands:\n  check <path>...\n  double-run -- <command> [args...]",
                env!("CARGO_PKG_VERSION")
            );
            Ok(())
        }
    }
}

fn check(args: &[String]) -> io::Result<()> {
    if args.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cc-detlint check requires at least one path",
        ));
    }
    let mut files = Vec::new();
    for path in args {
        collect_rust_files(Path::new(path), &mut files)?;
    }
    files.sort();
    let mut violations = Vec::new();
    for path in &files {
        let text = fs::read_to_string(path)?;
        for (line_index, line) in text.lines().enumerate() {
            for (needle, reason) in FORBIDDEN {
                if line.contains(needle) {
                    violations.push(format!(
                        "{}:{}: {needle} ({reason})",
                        path.display(),
                        line_index + 1,
                    ));
                }
            }
            if line.contains(" as usize") && line.contains("ptr") {
                violations.push(format!(
                    "{}:{}: pointer-derived integer ordering",
                    path.display(),
                    line_index + 1,
                ));
            }
        }
    }
    if violations.is_empty() {
        println!("cc-detlint: PASS files={}", files.len());
        Ok(())
    } else {
        for violation in &violations {
            eprintln!("{violation}");
        }
        Err(io::Error::other(format!(
            "cc-detlint found {} violation(s)",
            violations.len()
        )))
    }
}

fn collect_rust_files(path: &Path, files: &mut Vec<PathBuf>) -> io::Result<()> {
    if path.is_file() {
        if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path.to_owned());
        }
        return Ok(());
    }
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        if matches!(name.to_str(), Some("target" | ".git" | "node_modules")) {
            continue;
        }
        collect_rust_files(&entry.path(), files)?;
    }
    Ok(())
}

fn double_run(args: &[String]) -> io::Result<()> {
    let command_start = args
        .iter()
        .position(|arg| arg == "--")
        .map_or(0, |index| index + 1);
    let command = args.get(command_start).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "cc-detlint double-run requires -- <command>",
        )
    })?;
    let command_args = &args[command_start + 1..];
    let first = Command::new(command).args(command_args).output()?;
    let second = Command::new(command).args(command_args).output()?;
    if !first.status.success() || !second.status.success() {
        return Err(io::Error::other(format!(
            "double-run command failed: first={} second={}",
            first.status, second.status
        )));
    }
    if first.stdout != second.stdout {
        let stdout_index = first
            .stdout
            .iter()
            .zip(&second.stdout)
            .position(|(left, right)| left != right)
            .unwrap_or(first.stdout.len().min(second.stdout.len()));
        return Err(io::Error::other(format!(
            "double-run diverged at stdout byte {stdout_index}: first={} bytes second={} bytes",
            first.stdout.len(),
            second.stdout.len()
        )));
    }
    println!("cc-detlint double-run: PASS bytes={}", first.stdout.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scanner_finds_forbidden_sources() {
        let directory = env::temp_dir().join(format!("cc-detlint-{}", std::process::id()));
        fs::create_dir_all(&directory).expect("test directory");
        let file = directory.join("lib.rs");
        fs::write(&file, "use std::collections::HashMap;\n").expect("test input");
        let mut files = Vec::new();
        collect_rust_files(&directory, &mut files).expect("collect");
        assert_eq!(files, vec![file]);
        fs::remove_dir_all(directory).expect("remove test directory");
    }
}
