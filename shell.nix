{ pkgs ? import
    (
      fetchTarball
        {
          name = "21.11-2022-03-08";
          url = "https://github.com/NixOS/nixpkgs/archive/bb3dee861440695ce6d8f43d0dd3a97622cb08c4.tar.gz";
          sha256 = "0j94fz656a0xf3s7kvi8p16p52186ks6r3m1gv8i2zmiinlvv5v3";
        })
    { }
}:
let
  python = pkgs.python38;
  projectDir = builtins.path { path = ./.; name = "geostore"; };

  poetryEnv = pkgs.poetry2nix.mkPoetryEnv {
    inherit python projectDir;
    extraPackages = ps: [ ps.pip ];
    overrides = pkgs.poetry2nix.overrides.withDefaults (self: super: {
      typer = super.typer.overridePythonAttrs (
        old: {
          buildInputs = (old.buildInputs or [ ]) ++ [
            self.flit-core
          ];
        }
      );
    });
  };
in
poetryEnv.env.overrideAttrs (
  oldAttrs: {
    buildInputs = [
      pkgs.cacert
      pkgs.cargo
      pkgs.docker
      pkgs.gitFull
      pkgs.nodejs
      (pkgs.poetry.override {
        inherit python;
      })
      pkgs.which
    ];
    shellHook = ''
      . ${projectDir + "/activate-dev-env.bash"}
      ln --force --no-dereference --symbolic ${poetryEnv.interpreter} ./.run/python
      cat <<'EOF'
      Welcome to the Geostore development environment!

      Please run `npm install` to install Node.js packages, if you haven't already.

      You should now be able to run `cdk` and `pytest`.
      EOF
    '';
  }
)
