// SPDX-License-Identifier: Apache-2.0
//! CLI entry point for collection, configuration checks, and sample output.
//!
//! `show-payload` runs a built-in sample through the egress pipeline with a test key.
//! It does not validate a live deployment. Collection uses the engine-neutral source interface.

mod cli;

use std::process::ExitCode;

use cli::demo::show_payload;
use cli::opts::{help, Command, Opts};
use cli::run::run_live;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let opts = match Opts::parse(&args) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("infertap: {e}\n\n{}", help());
            return ExitCode::from(2);
        }
    };
    if opts.command == Command::Help {
        print!("{}", help());
        return ExitCode::SUCCESS;
    }
    if opts.command == Command::Version {
        println!("{}", cli::version_line());
        return ExitCode::SUCCESS;
    }
    cli::logging::init(
        opts.log_level.unwrap_or(cli::logging::DEFAULT_LEVEL),
        opts.log_format,
    );
    match opts.command {
        Command::ShowPayload => show_payload(),
        Command::CheckConfig => cli::run::check_config(&opts),
        Command::Probe => cli::probe::run(&opts),
        Command::Run => run_live(&opts),
        Command::Help | Command::Version => ExitCode::SUCCESS,
    }
}
