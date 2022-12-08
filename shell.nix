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
      aws-cdk-asset-awscli-v1 = super.aws-cdk-asset-awscli-v1.overridePythonAttrs (
        old: {
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.setuptools ];
        }
      );
      aws-cdk-asset-kubectl-v20 = super.aws-cdk-asset-kubectl-v20.overridePythonAttrs (
        old: {
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.setuptools ];
        }
      );
      aws-cdk-asset-node-proxy-agent-v5 = super.aws-cdk-asset-node-proxy-agent-v5.overridePythonAttrs (
        old: {
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.setuptools ];
        }
      );
      aws-cdk-aws-batch-alpha = super.aws-cdk-aws-batch-alpha.overridePythonAttrs (
        old: {
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.setuptools ];
        }
      );
      aws-cdk-aws-lambda-python-alpha = super.aws-cdk-aws-lambda-python-alpha.overridePythonAttrs (
        old: {
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.setuptools ];
        }
      );
      aws-cdk-lib = super.aws-cdk-lib.overridePythonAttrs (
        old: {
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.setuptools ];
        }
      );
      constructs = super.constructs.overridePythonAttrs (
        old: {
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.setuptools ];
        }
      );
      jsii = super.jsii.overridePythonAttrs (
        old: {
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.setuptools ];
        }
      );
      mypy-boto3-ssm = super.mypy-boto3-ssm.overridePythonAttrs (
        old: {
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.setuptools ];
        }
      );
      mypy-boto3-stepfunctions = super.mypy-boto3-stepfunctions.overridePythonAttrs (
        old: {
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.setuptools ];
        }
      );
      mypy-boto3-sts = super.mypy-boto3-sts.overridePythonAttrs (
        old: {
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.setuptools ];
        }
      );
      pystac = super.pystac.overridePythonAttrs (
        old: {
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.setuptools ];
        }
      );
      python-ulid = super.python-ulid.override {
        preferWheel = true;
      };
      types-pkg-resources = super.types-pkg-resources.overridePythonAttrs (
        old: {
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.setuptools ];
        }
      );
      types-python-dateutil = super.types-python-dateutil.overridePythonAttrs (
        old: {
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.setuptools ];
        }
      );
      types-six = super.types-six.overridePythonAttrs (
        old: {
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ self.setuptools ];
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
      pkgs.gitlint
      pkgs.go
      pkgs.isort
      pkgs.mutmut
      pkgs.mypy
      pkgs.nodejs
      (pkgs.poetry.override {
        inherit python;
      })
      pkgs.pre-commit
      pkgs.pylint
      pkgs.python39Packages.black
      pkgs.python39Packages.coverage
      pkgs.python39Packages.boto3
      pkgs.python39Packages.ipdb
      pkgs.python39Packages.pip
      pkgs.python39Packages.pip-tools
      pkgs.python39Packages.pytest
      pkgs.python39Packages.pytest-randomly
      pkgs.python39Packages.pytest-socket
      pkgs.python39Packages.pytest-subtests
      pkgs.python39Packages.pytest-timeout
      pkgs.python39Packages.types-python-dateutil
      pkgs.python39Packages.types-requests
      pkgs.python39Packages.types-setuptools
      pkgs.python39Packages.types-toml
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
