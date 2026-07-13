#[path = "../../prebuilt_guest.rs"]
mod prebuilt_guest;

fn main() {
    if prebuilt_guest::embed(env!("CARGO_PKG_NAME")) {
        return;
    }
    risc0_build::embed_methods();
}
