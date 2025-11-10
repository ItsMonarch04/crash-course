// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

fn main() {
    let mut seed = String::from("0x2a");
    let mut profile = String::from("calm");
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--seed" => {
                if let Some(value) = args.next() {
                    seed = value;
                }
            }
            "--profile" => {
                if let Some(value) = args.next() {
                    profile = value;
                }
            }
            _ => {}
        }
    }
    let spec = format!("{{\"seed\":\"{seed}\",\"profile\":\"{profile}\"}}");
    let mut handle = cc_wasm::init(&spec);
    let mut state = cc_wasm::state(&handle);
    for _ in 0..120 {
        state = cc_wasm::step(&mut handle, 500_000_000);
    }
    print!("{state}");
}
