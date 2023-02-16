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
      filelock = super.filelock.overridePythonAttrs (
        # In poetry2nix >1.39.1
        old: {
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.hatchling self.hatch-vcs ];
        }
      );
      python-ulid = super.python-ulid.overridePythonAttrs (
        # In poetry2nix >1.39.1
        old: {
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.setuptools-scm ];
        }
      );
      virtualenv = super.virtualenv.overridePythonAttrs (
        # https://github.com/nix-community/poetry2nix/pull/985
        old: {
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.hatchling self.hatch-vcs ];
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
      pkgs.go
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
      ln --force --no-dereference --symbolic ${poetryEnv} .venv
      cat <<'EOF'
      Welcome to the Geostore development environment!

      Please run `npm install` to install Node.js packages, if you haven't already.

      You should now be able to run `cdk` and `pytest`.
      EOF
    '';
  }
)
