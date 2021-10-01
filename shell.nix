let
  pkgs = import
    (
      fetchTarball {
        name = "21.05";
        url = "https://github.com/NixOS/nixpkgs/archive/7e9b0dff974c89e070da1ad85713ff3c20b0ca97.tar.gz";
        sha256 = "1ckzhh24mgz6jd1xhfgx0i9mijk6xjqxwsshnvq789xsavrmsc36";
      })
    { };

  nodejsVersion = pkgs.lib.fileContents ./.nvmrc;
  buildNodeJs = pkgs.callPackage "${toString pkgs.path}/pkgs/development/web/nodejs/nodejs.nix" {
    python = pkgs.python38;
  };
  nodejs = buildNodeJs {
    version = nodejsVersion;
    sha256 = "1k6bgs83s5iaawi63dcc826g23lfqr13phwbbzwx0pllqcyln49j";
  };

  poetryEnv = pkgs.poetry2nix.mkPoetryEnv {
    python = pkgs.python38;
    projectDir = builtins.path { path = ./.; name = "geostore"; };
    overrides = pkgs.poetry2nix.overrides.withoutDefaults (self: super: {
      astroid = super.astroid.overridePythonAttrs (
        old: rec {
          buildInputs = (old.buildInputs or [ ]) ++ [ self.typing-extensions ];
        }
      );

      black = super.black.overridePythonAttrs (
        old: {
          dontPreferSetupPy = true;
        }
      );

      importlib-metadata = super.importlib-metadata.overridePythonAttrs (old: {
        postPatch = ''
          substituteInPlace setup.py --replace 'setuptools.setup()' 'setuptools.setup(version="${old.version}")'
        '';
      });

      mccabe = super.mccabe.overridePythonAttrs (
        old: {
          buildInputs = (old.buildInputs or [ ]) ++ [ self.pytest-runner ];
        }
      );

      platformdirs = super.platformdirs.overridePythonAttrs (old: {
        postPatch = ''
          substituteInPlace setup.py --replace 'setup()' 'setup(version="${old.version}")'
        '';
      });

      pylint = super.pylint.overridePythonAttrs (
        old: {
          buildInputs = (old.buildInputs or [ ]) ++ [ self.pytest-runner ];
          propagatedBuildInputs = (old.propagatedBuildInputs or [ ]) ++ [ self.typing-extensions ];
        }
      );

      pytest = super.pytest.overridePythonAttrs (
        old: {
          # Fixes https://github.com/pytest-dev/pytest/issues/7891
          postPatch = old.postPatch or "" + ''
            sed -i '/\[metadata\]/aversion = ${old.version}' setup.cfg
          '';
        }
      );

      pytest-randomly = super.pytest-randomly.overrideAttrs (old: {
        propagatedBuildInputs = (old.propagatedBuildInputs or [ ]) ++ [
          self.importlib-metadata
        ];
      });

      ruamel-yaml = super.ruamel-yaml.overridePythonAttrs (
        old: {
          propagatedBuildInputs = (old.propagatedBuildInputs or [ ]) ++ [ self.ruamel-yaml-clib ];
        }
      );

      zipp = super.zipp.overridePythonAttrs (old: {
        postPatch = ''
          substituteInPlace setup.py --replace 'setuptools.setup()' 'setuptools.setup(version="${old.version}")'
        '';
      });
    });
  };
in
pkgs.mkShell {
  buildInputs = [
    nodejs
    pkgs.cargo
    pkgs.nodePackages.aws-azure-login
    pkgs.python38Packages.pip
    pkgs.python38Packages.poetry
    poetryEnv
  ];
  shellHook = ''
        . activate-dev-env.bash
        ln --force --symbolic "$(type -p python)" .run/python
        cat <<'EOF'
    Welcome to the Geostore development environment!

    Please run `npm install` to install Node.js packages, if you haven't already.

    You should now be able to run `aws-azure-login`, `cdk` and `pytest`.
    EOF
  '';
}
