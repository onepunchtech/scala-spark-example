{
  description = "my project description";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs";
  inputs.flake-utils.url = "github:numtide/flake-utils";

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        version = "0.0.3";
        pkgs = nixpkgs.legacyPackages.${system};

        myDevTools = [
          pkgs.scala
          pkgs.sbt
          pkgs.metals
        ];

      in {
        devShells.default = pkgs.mkShell {
          buildInputs = myDevTools;
          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath myDevTools;
        };

      });
}
