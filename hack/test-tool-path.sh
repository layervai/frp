#!/usr/bin/env bash
set -euo pipefail
export GOENV=off

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
fixture=$(mktemp -d)
trap 'rm -rf -- "$fixture"' EXIT
mkdir -p "$fixture/module"
cat > "$fixture/module/go.mod" <<'MOD'
module example.invalid/frp-path-contract

go 1.23
MOD
cat > "$fixture/module/main.go" <<'GO'
package main

import "fmt"

func main() { fmt.Print("installed-tool-found") }
GO

# Exercise Go's actual install destination, including a multi-entry GOPATH.
for mode in gobin gopath; do
  export GOPATH="$fixture/$mode/first path:$fixture/$mode/second"
  if [[ "$mode" == gobin ]]; then
    export GOBIN="$fixture/explicit-bin"
  else
    export GOBIN=""
  fi
  (cd "$fixture/module" && go install .)
  result=$(cd "$repo_root" && make --no-print-directory -s -f Makefile -f - frp-path-contract <<'MAKE'
.PHONY: frp-path-contract
frp-path-contract:
	@printf '%s' "$$(frp-path-contract)"
MAKE
)
  [[ "$result" == installed-tool-found ]] || { echo "installed Go tool is not on Make's PATH ($mode)" >&2; exit 1; }
done
