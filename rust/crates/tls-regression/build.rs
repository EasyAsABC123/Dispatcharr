// Original SQLx/rustls authors retain all library and algorithm credit.
// The library source lives outside this small regression package. Watch it
// explicitly so one shared target cannot reuse an older verifier test binary.
fn main() {
    println!("cargo:rerun-if-changed=../../vendor/sqlx-core-0.8.6/src");
}
