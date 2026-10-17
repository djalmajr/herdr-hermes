#!/bin/sh
# Install or update the nau binary and the optional Herdr plugin on
# macOS and Linux. Re-running the script with another --ref updates the
# node to that ref, binary and plugin on the same ref.
#
# Prerequisites: Go 1.26 or newer on the PATH; for the plugin step,
# Herdr on the PATH. The script never prints environment variables or
# configuration contents.

set -eu

REF="main"
BIN_DIR="$HOME/.local/bin"
MACHINE_LABEL=""
NO_PLUGIN=0
PLUGIN_ID="djalmajr.herdr-hermes"
PLUGIN_SOURCE="djalmajr/nau/plugin"
MODULE_PATH="github.com/djalmajr/nau/cmd/nau"

usage() {
    cat <<'EOF'
Usage: sh scripts/install.sh [options]

Install or update the nau binary into the bin directory and the Herdr
plugin, both from the same ref. Updating is re-running the script with
another --ref.

Options:
  --ref <ref>            ref to install (default: main)
  --bin-dir <dir>        bin directory (default: $HOME/.local/bin)
  --machine-label <lbl>  set machine_label when it is still empty
  --no-plugin            skip the Herdr plugin step
  -h, --help             show this help
EOF
}

while [ "$#" -gt 0 ]; do
    case "$1" in
    --ref)
        if [ "$#" -lt 2 ]; then
            echo "nau install: --ref requires a value" >&2
            exit 2
        fi
        REF="$2"
        shift 2
        ;;
    --bin-dir)
        if [ "$#" -lt 2 ]; then
            echo "nau install: --bin-dir requires a value" >&2
            exit 2
        fi
        BIN_DIR="$2"
        shift 2
        ;;
    --machine-label)
        if [ "$#" -lt 2 ]; then
            echo "nau install: --machine-label requires a value" >&2
            exit 2
        fi
        MACHINE_LABEL="$2"
        shift 2
        ;;
    --no-plugin)
        NO_PLUGIN=1
        shift
        ;;
    -h | --help)
        usage
        exit 0
        ;;
    *)
        echo "nau install: unknown option: $1" >&2
        usage >&2
        exit 2
        ;;
    esac
done

if ! command -v go >/dev/null 2>&1; then
    echo "nau install: go not found on the PATH; install Go 1.26 or newer first" >&2
    exit 1
fi

# A dedicated temp dir, removed on any exit: the fresh binary is built
# there before the installed one is touched.
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "nau install: installing $MODULE_PATH@$REF"
GOBIN="$TMP" GOPROXY=direct go install "$MODULE_PATH@$REF"

# Test the new binary before touching the installed one.
echo "nau install: checking the new binary"
if ! "$TMP/nau" version >/dev/null; then
    echo "nau install: the new binary failed 'nau version'; the installed binary is untouched" >&2
    exit 1
fi

# Replace the installed binary: copy to a sibling temp name and rename
# over it, so the swap is atomic within the same directory.
mkdir -p "$BIN_DIR"
STAGE="$BIN_DIR/.nau.new.$$"
cp "$TMP/nau" "$STAGE"
mv "$STAGE" "$BIN_DIR/nau"
NAU="$BIN_DIR/nau"

# Set machine_label only when it is still empty; never print the value.
if [ -n "$MACHINE_LABEL" ]; then
    CURRENT="$("$NAU" config get machine_label 2>/dev/null)" || CURRENT=""
    CURRENT_VALUE="$(printf '%s\n' "$CURRENT" | sed -n 's/.*"value":"\([^"]*\)".*/\1/p')"
    if [ -z "$CURRENT_VALUE" ]; then
        "$NAU" config set machine_label "$MACHINE_LABEL"
        echo "nau install: machine_label set" >&2
    else
        echo "nau install: machine_label is already set; keeping the current value" >&2
    fi
fi

# The Herdr plugin, unless skipped or Herdr is absent.
if [ "$NO_PLUGIN" -eq 1 ]; then
    echo "nau install: skipping the Herdr plugin (--no-plugin)" >&2
else
    if ! command -v herdr >/dev/null 2>&1; then
        echo "nau install: herdr not found on the PATH; skipping the Herdr plugin" >&2
    else
        LIST="$(herdr plugin list --plugin "$PLUGIN_ID" --json)"
        case "$LIST" in
        *'"plugin_id":"djalmajr.herdr-hermes"'*)
            # Installed: a local origin (herdr plugin link) is refused,
            # a GitHub origin is updated in place.
            KIND="$(printf '%s\n' "$LIST" | sed -n 's/.*"kind":"\([^"]*\)".*/\1/p')"
            if [ -z "$KIND" ]; then
                echo "nau install: could not determine the source of the installed herdr plugin $PLUGIN_ID; if it is linked to a local path, run 'herdr plugin unlink $PLUGIN_ID' first" >&2
                exit 1
            fi
            if [ "$KIND" != "github" ]; then
                echo "nau install: the herdr plugin $PLUGIN_ID is linked to a local path; run 'herdr plugin unlink $PLUGIN_ID' first and re-run this script" >&2
                exit 1
            fi
            ;;
        esac
        herdr plugin install "$PLUGIN_SOURCE" --ref "$REF" -y
        echo "nau install: herdr plugin $PLUGIN_ID installed from $PLUGIN_SOURCE (ref $REF)" >&2
    fi
fi

# Warn, without failing, when the bin dir is not on the PATH.
case ":$PATH:" in
*":$BIN_DIR:"*) ;;
*)
    echo "nau install: warning: $BIN_DIR is not on the PATH; add it to use nau and the plugin" >&2
    ;;
esac

echo "nau install: installed" >&2
"$NAU" version
# doctor is informational: its non-zero exit (for example a missing
# backend) must not fail the install.
if ! "$NAU" doctor; then
    echo "nau install: doctor reported a problem (informational); see the output above" >&2
fi
