let
  pkgs = import
    (
      fetchTarball {
        # TODO: Pin to stable nixpkgs
        name = "nixpkgs-2021-10-01";
        url = "https://github.com/NixOS/nixpkgs/archive/9a23237031b385945132c8dac7d7ad97ece67408.tar.gz";
        sha256 = "0g5ksps5pdhh1npv5vs6560gn0cdbvs536p54nm87lyvz50x7f6m";
      })
    { };

  nodejsVersion = pkgs.lib.fileContents ./.nvmrc;
  buildNodeJs = pkgs.callPackage "${toString pkgs.path}/pkgs/development/web/nodejs/nodejs.nix" { };
  nodejs = buildNodeJs {
    enableNpm = true;
    version = nodejsVersion;
    sha256 = "1vf989canwcx0wdpngvkbz2x232yccp7fzs1vcbr60rijgzmpq2n";
  };

  poetryEnv = pkgs.poetry2nix.mkPoetryEnv {
    python = pkgs.python38;
    projectDir = builtins.path { path = ./.; name = "geostore"; };
  };
in
pkgs.mkShell {
  buildInputs = [
    nodejs
    poetryEnv
    poetryEnv.python.pkgs.pip
    poetryEnv.python.pkgs.poetry
  ];
  shellHook = ''
    . activate-dev-env.bash
  '';
}
