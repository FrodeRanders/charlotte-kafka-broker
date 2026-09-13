fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=BROKER_ADVERTISE_HOST");
    println!("cargo:rerun-if-env-changed=BROKER_ADVERTISE_PORT");
}
