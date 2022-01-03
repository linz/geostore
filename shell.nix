{ pkgs ? import
    (
      fetchTarball
        {
          name = "nixpkgs-21.11-2022-01-03";
          url = "https://github.com/NixOS/nixpkgs/archive/08370e1e271f6fe00d302bebbe510fe0e2c611ca.tar.gz";
          sha256 = "1s9g0vry5jrrvvna250y538i99zy12xy3bs7m3gb4iq64qhyd6bq";
        })
    { }
}:
let
  projectDir = builtins.path { path = ./.; name = "geostore"; };
  nodejsVersion = pkgs.lib.fileContents (projectDir + "/.nvmrc");
  buildNodeJs = pkgs.callPackage "${toString pkgs.path}/pkgs/development/web/nodejs/nodejs.nix" {
    python = pkgs.python38;
  };
  nodejs = buildNodeJs {
    version = nodejsVersion;
    sha256 = "1k6bgs83s5iaawi63dcc826g23lfqr13phwbbzwx0pllqcyln49j";
  };

  poetryEnv = pkgs.poetry2nix.mkPoetryEnv {
    python = pkgs.python38;
    inherit projectDir;
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

      importlib-resources = super.importlib-resources.overridePythonAttrs (
        old: {
          dontPreferSetupPy = true;
        }
      );

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

      tomli = super.tomli.overridePythonAttrs (old: {
        nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.flit-core ];
      });

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
        . ${projectDir + "/activate-dev-env.bash"}
        ln --force --symbolic ${poetryEnv.python.interpreter} ${projectDir}/.run/python
        cat <<'EOF'
    Welcome to the Geostore development environment!

    Please run `npm install` to install Node.js packages, if you haven't already.

    You should now be able to run `aws-azure-login`, `cdk` and `pytest`.
    EOF
  '';
}
