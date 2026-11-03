{
  description = "OmniGraph CLI, server and Azure admission wrapper";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    naersk = {
      url = "github:nix-community/naersk";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.fenix.follows = "fenix";
    };
  };

  outputs = { self, nixpkgs, fenix, naersk }:
    let
      # The release archive targets (.github/workflows/build-release-archives.yml)
      # other than Windows.
      systems = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f system nixpkgs.legacyPackages.${system});
    in
    {
      packages = forAllSystems (system: pkgs:
        let
          inherit (pkgs) lib;
          # The compiler rust-toolchain.toml pins. nixpkgs' own rustc links an
          # LLVM older than 22, which cannot lower Lance's AVX-512 intrinsics.
          # The hash is that channel's manifest, so it changes with every
          # toolchain bump.
          toolchain = fenix.packages.${system}.fromToolchainFile {
            file = ./rust-toolchain.toml;
            sha256 = "sha256-A1abGIbOtcBSdrUMhDGrER3pRM1hQP4fp9gh3Y4PKc8=";
          };
          naersk' = pkgs.callPackage naersk {
            cargo = toolchain;
            rustc = toolchain;
          };
          # The release archives' flags: none, except long calls on arm64
          # macOS, whose text exceeds the +/-128 MiB direct-branch range.
          releaseFlags = lib.optionalString pkgs.stdenv.hostPlatform.isDarwin "-C code-model=large";
        in
        {
          default = naersk'.buildPackage {
            # naersk names a workspace build `rust-workspace` without `name`.
            name = "omnigraph";
            version = (builtins.fromTOML (builtins.readFile ./crates/omnigraph-cli/Cargo.toml)).package.version;
            src = ./.;
            # The packages and features of the release archives
            # (build-release-archives.yml).
            cargoBuildOptions = options: options ++ [
              "--locked"
              "-p" "omnigraph-cli"
              "-p" "omnigraph-server"
              "-p" "omnigraph-azure-admission"
              "--features" "omnigraph-server/aws"
            ];
            nativeBuildInputs = [ pkgs.protobuf ];
            # A set RUSTFLAGS replaces the workspace `[build] rustflags`, the
            # test suites' `--cfg tokio_unstable`, as in every release build.
            # naersk would put its path remapping in CARGO_BUILD_RUSTFLAGS,
            # which Cargo merges with that list instead, so the remapping is
            # spelled here. It keeps the vendored sources out of the closure.
            remapPathPrefix = false;
            preBuild = ''
              export RUSTFLAGS="--remap-path-prefix $cratesio_sources=/sources --remap-path-prefix $git_sources=/sources ${releaseFlags}"
            '';
            # Stamped on the top-level build only, so a new commit does not
            # rebuild the dependencies. A dirty tree has no revision and,
            # like a source build, reports no commit.
            overrideMain = _: lib.optionalAttrs (self ? rev) {
              OMNIGRAPH_SOURCE_VERSION = self.rev;
            };
            meta = {
              description = "OmniGraph CLI, server and Azure admission wrapper";
              homepage = "https://github.com/ModernRelay/omnigraph";
              license = lib.licenses.mit;
              mainProgram = "omnigraph";
            };
          };
        });

      checks = forAllSystems (system: _: {
        default = self.packages.${system}.default;
      });
    };
}
