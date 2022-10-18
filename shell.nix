{ pkgs ? import
    (
      fetchTarball (
        builtins.fromJSON (
          builtins.readFile ./nixpkgs.json)))
    { }
}:
let
  python = pkgs.python39;
  projectDir = builtins.path { path = ./.; name = "geostore"; };

  poetryEnv = pkgs.poetry2nix.mkPoetryEnv {
    inherit python projectDir;
    overrides = pkgs.poetry2nix.overrides.withDefaults (self: super: {
      idna = super.idna.overridePythonAttrs (
        old: {
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.flit-core ];
        }
      );
      jsonschema = super.jsonschema.overridePythonAttrs (
        old: {
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.hatch-fancy-pypi-readme ];
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
      pkgs.python39Packages.pip
      pkgs.python39Packages.pip-tools
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
