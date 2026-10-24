{
  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    naersk.url = "github:nix-community/naersk";
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    
    nixpkgs-mozilla = {
      url = "github:mozilla/nixpkgs-mozilla";
      flake = false;
    };
  };
  outputs = { self, flake-utils, naersk, nixpkgs, nixpkgs-mozilla }:
    flake-utils.lib.eachDefaultSystem (system:
  let
    pkgs = (import nixpkgs) {
      inherit system;
      overlays = [ (import nixpkgs-mozilla) ];
    };
    toolchain = (pkgs.rustChannelOf {
      rustToolchain = ./rust-toolchain.toml;
      sha256 = "sha256-A1abGIbOtcBSdrUMhDGrER3pRM1hQP4fp9gh3Y4PKc8=";
    }).rust;
    naersk' = pkgs.callPackage naersk {
      cargo = toolchain;
      rustc = toolchain;
    };
  in {
    packages.default = naersk'.buildPackage {
      pname = "omnigraph";
      version = "0.14.0";
      src = ./.;
      nativeBuildInputs = with pkgs; [
        pkg-config
        protobuf
        clang
        rustc
        cargo
      ];
    };
  });
}
