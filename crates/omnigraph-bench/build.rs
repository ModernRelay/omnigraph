fn main() {
    let profile = std::env::var("PROFILE").expect("Cargo sets PROFILE for build scripts");
    let opt_level = std::env::var("OPT_LEVEL").expect("Cargo sets OPT_LEVEL for build scripts");
    println!("cargo:rustc-env=OMNIGRAPH_BENCH_BUILD_PROFILE={profile}");
    println!("cargo:rustc-env=OMNIGRAPH_BENCH_BUILD_OPT_LEVEL={opt_level}");
}
