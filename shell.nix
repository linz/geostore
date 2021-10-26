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
    pkgs.cargo
    pkgs.nodejs-14_x
    pkgs.nodePackages.aws-azure-login
    pkgs.python38Packages.pip
    pkgs.python38Packages.poetry
    poetryEnv
  ];
  shellHook = ''
        . activate-dev-env.bash
        cat <<'EOF'
    Welcome to the Geostore development environment!

    Please run `npm install` to install Node.js packages, if you haven't already.

    You should now be able to run `aws-azure-login`, `cdk` and `pytest`.
    EOF
  '';
}
