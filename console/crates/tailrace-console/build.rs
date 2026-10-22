//! T27 self-contained packaging: materialize the crate-local `dist/` that
//! rust-embed embeds (src/spa.rs). Three situations, one precedence:
//!
//! 1. repository web build present (`../../../web/dist`, from `pnpm build`)
//!    → mirror it into `./dist` so every console build embeds the fresh SPA
//!    (dev flow unchanged: build.sh runs `pnpm build`, then cargo embeds);
//! 2. web build absent but `./dist` populated → keep it as-is — this is the
//!    packaged copy inside a published crate (cargo install / package
//!    verification build outside the repository);
//! 3. nothing available → write the placeholder index.html (same caliber as
//!    the CI stub at .github/workflows/ci.yml:26) so rust-embed always has
//!    a folder to compile against.
//!
//! The real SPA enters published tarballs via `scripts/release-crates.sh`
//! (materializes dist/ BEFORE the packaging file walk) plus the explicit
//! `include` list in Cargo.toml, which overrides the gitignore on dist/.

use std::fs;
use std::path::Path;

fn main() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR set");
    let root = Path::new(&manifest_dir);
    let web_dist = root.join("../../../web/dist");
    let local = root.join("dist");

    // Watch the web/ root (always checked out) rather than web/dist itself:
    // cargo only fingerprints watched paths that EXIST at run time, so a
    // watch on web/dist goes dead on a tree that never built the SPA, and
    // the first `pnpm build` would then fail to re-trigger the mirror.
    // The directory watch covers dist/ appearing, changing or vanishing.
    println!(
        "cargo:rerun-if-changed={}",
        web_dist.parent().expect("web/dist has a parent").display()
    );

    if web_dist.join("index.html").exists() {
        if local.exists() {
            fs::remove_dir_all(&local).expect("clear stale crate-local dist/");
        }
        copy_dir(&web_dist, &local);
    } else if !local.join("index.html").exists() {
        fs::create_dir_all(&local).expect("create dist/");
        fs::write(
            local.join("index.html"),
            "<!doctype html><title>placeholder</title>\n",
        )
        .expect("write placeholder index.html");
    }
    // else: packaged/materialized dist stays as-is (situation 2).
}

/// Minimal recursive copy, std-only — no build-dependencies wanted here.
fn copy_dir(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap_or_else(|e| panic!("create {}: {e}", dst.display()));
    for entry in fs::read_dir(src).unwrap_or_else(|e| panic!("read {}: {e}", src.display())) {
        let entry = entry.unwrap_or_else(|e| panic!("read {} entry: {e}", src.display()));
        let from = entry.path();
        if entry.file_type().expect("dist entry type").is_dir() {
            copy_dir(&from, &dst.join(entry.file_name()));
        } else {
            let to = dst.join(entry.file_name());
            fs::copy(&from, &to).unwrap_or_else(|e| panic!("copy {}: {e}", from.display()));
        }
    }
}
