{ pkgs ? import
    (
      fetchTarball
        {
          name = "22.05";
          url = "https://github.com/NixOS/nixpkgs/archive/ce6aa13369b667ac2542593170993504932eb836.tar.gz";
          sha256 = "0d643wp3l77hv2pmg2fi7vyxn4rwy0iyr8djcw1h5x72315ck9ik";
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
