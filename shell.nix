let
  pkgs = import
    (
      fetchTarball {
        # TODO: Pin to stable nixpkgs
        name = "nixpkgs-2021-09-28";
        url = "https://github.com/NixOS/nixpkgs/archive/ed8c752e13ef5a217806556a96b51ca7f7fb1007.tar.gz";
        sha256 = "03yharwv0lal286d3zy6b7kj4px111s5h3a8nar8banpnqgml7v5";
      })
    { };
  poetryEnv = pkgs.poetry2nix.mkPoetryEnv {
    python = pkgs.python38;
    projectDir = builtins.path { path = ./.; name = "geostore"; };
  };
in
pkgs.mkShell {
  buildInputs = [
    pkgs.nodejs-14_x
    poetryEnv
    pkgs.poetry
  ];
  shellHook = ''
    . activate-dev-env.bash
  '';
}
