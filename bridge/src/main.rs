mod protocol;
mod runtime;
mod schema;

fn main() -> std::io::Result<()> {
    protocol::run(std::io::stdin().lock(), std::io::stdout().lock())
}
