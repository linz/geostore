{ pkgs ? import
    (
      fetchTarball
        {
          name = "nixos-22.11pre405560.2da64a81275";
          url = "https://github.com/NixOS/nixpkgs/archive/2da64a81275b68fdad38af669afeda43d401e94b.tar.gz";
          sha256 = "1k71lmzdaa48yqkmsnd22n177qmxxi4gj2qcmdbv0mc6l4f27wd0";
        })
    { }
}:
let
  python = pkgs.python38;
  projectDir = builtins.path { path = ./.; name = "geostore"; };

  poetryEnv = pkgs.poetry2nix.mkPoetryEnv {
    inherit python projectDir;
    overrides = pkgs.poetry2nix.overrides.withDefaults (self: super: {
      linz-logger = super.linz-logger.overridePythonAttrs (
        old: {
          buildInputs = (old.buildInputs or [ ]) ++ [
            self.poetry
          ];
        }
      );
      mypy = super.mypy.overridePythonAttrs (old: {
        patches = [ ];
        MYPY_USE_MYPYC = false;
      });
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
      pkgs.python38Packages.pip
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
