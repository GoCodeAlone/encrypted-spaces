#![deny(clippy::print_stdout)]

mod protocol;
mod runtime;
mod schema;

fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn"))
        .target(env_logger::Target::Stderr)
        .init();
    log::debug!("bridge diagnostics initialized");

    let mut args = std::env::args_os();
    let _program = args.next();
    if args.next().is_some_and(|arg| arg == "--version") && args.next().is_none() {
        print_version();
        return Ok(());
    }
    protocol::run(std::io::stdin(), std::io::stdout())
}

#[allow(clippy::print_stdout)]
fn print_version() {
    println!("encrypted-spaces-bridge {}", env!("CARGO_PKG_VERSION"));
}
