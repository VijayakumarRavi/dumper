{
  description = "Dumper - A tiny, production-grade database backup, restore, and repository-management CLI in Rust";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config = {
            permittedInsecurePackages = [
              "minio-2025-10-15T17-29-55Z"
            ];
          };
        };

        dumperPkg = pkgs.rustPlatform.buildRustPackage {
          pname = "dumper";
          version = "0.1.0";
          src = ./.;
          cargoLock = {
            lockFile = ./Cargo.lock;
          };
          nativeBuildInputs = [ pkgs.pkg-config ];
          buildInputs = [ pkgs.openssl pkgs.zstd ]
            ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isDarwin [
              pkgs.apple-sdk_15
              pkgs.libiconv
            ];
        };
      in
      {
        packages.default = dumperPkg;
        packages.dumper = dumperPkg;

        apps.default = flake-utils.lib.mkApp {
          drv = dumperPkg;
        };

        devShells.default = pkgs.mkShell {
          name = "dumper-devshell";

          packages = with pkgs; [
            # Rust toolchain & developer tools
            rustc
            cargo
            clippy
            rustfmt
            rust-analyzer
            cargo-audit
            cargo-watch

            # Build dependencies
            pkg-config
            openssl
            zstd

            # Integration testing servers & tools
            postgresql
            mariadb
            minio
            garage
          ] ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isDarwin [
            apple-sdk_15
            libiconv
          ];

          shellHook = ''
            export RUST_BACKTRACE=1
            export RUST_SRC_PATH="${pkgs.rustPlatform.rustLibSrc}"
            
            echo "=================================================================="
            echo " 🚀 Dumper Development Environment Active"
            echo " Rust: $(rustc --version)"
            echo " Cargo: $(cargo --version)"
            echo " Testing tools: postgresql, mariadb, minio, garage available"
            echo "=================================================================="
          '';
        };
      }
    );
}
