mod protocol;
mod runtime;
mod schema;

fn main() -> std::io::Result<()> {
    let mut args = std::env::args_os();
    let _program = args.next();
    if args.next().is_some_and(|arg| arg == "--version") && args.next().is_none() {
        println!("encrypted-spaces-bridge {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    protocol::run(std::io::stdin().lock(), std::io::stdout())
}
