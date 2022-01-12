{ pkgs ? import <nixpkgs> {} }:
  pkgs.mkShell {
    nativeBuildInputs = [ 
      pkgs.python39Packages.requests
      pkgs.python39Packages.ipython
      pkgs.python39Packages.tqdm
      pkgs.python39Packages.joblib
      pkgs.python39Packages.rich
    ];
}
