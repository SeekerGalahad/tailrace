# Targets per design doc §8.1: fmt / lint / test / web / build / release
.PHONY: fmt lint test web build release

fmt:
	cargo fmt --all

lint:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

web:
	cd web && pnpm install && pnpm build

build:
	./scripts/build.sh

release:
	./scripts/package.sh
