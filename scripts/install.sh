#!/bin/sh
# Install or update the nau binary and the optional Herdr plugin on
# macOS and Linux. Re-running the script with another --ref updates
# the node to that ref, binary and plugin on the same ref.
#
# On macOS, --serve installs (or rewrites) the a2a serve LaunchAgent
# and (re)starts it, and a re-run restarts the installed service after
# the binary swap, so the node never keeps running an old binary.
#
# Prerequisites: Go 1.26 or newer on the PATH; for the plugin step,
# Herdr on the PATH. The script never prints environment variables or
# configuration contents.

set -eu

REF="main"
BIN_DIR="$HOME/.local/bin"
MACHINE_LABEL=""
NO_PLUGIN=0
SERVE=0
SERVE_DRY_RUN=0
PLUGIN_ID="djalmajr.herdr-hermes"
PLUGIN_SOURCE="djalmajr/nau/plugin"
MODULE_PATH="github.com/djalmajr/nau/cmd/nau"
A2A_LABEL="djalmajr.nau.a2a-serve"
A2A_PLIST="$HOME/Library/LaunchAgents/$A2A_LABEL.plist"
A2A_LOG_DIR="$HOME/Library/Logs/nau"
A2A_LOG="$A2A_LOG_DIR/a2a-serve.log"

usage() {
    cat <<'EOF'
Usage: sh scripts/install.sh [options]

Install or update the nau binary into the bin directory and the Herdr
plugin, both from the same ref. Updating is re-running the script with
another --ref. On macOS, --serve installs the a2a serve LaunchAgent and
(re)starts it; every re-run restarts the installed service after the
binary swap, so the node never keeps running an old binary.

Options:
  --ref <ref>            ref to install (default: main)
  --bin-dir <dir>        bin directory (default: $HOME/.local/bin)
  --machine-label <lbl>  set machine_label when it is still empty
  --no-plugin            skip the Herdr plugin step
  --serve                install or rewrite the a2a serve LaunchAgent and (re)start it (macOS)
  --serve-dry-run        print the LaunchAgent plist that --serve would install and exit (macOS)
  -h, --help             show this help
EOF
}

# The bin directory as an absolute path, for the plist arguments: if
# it does not start with /, prefix it with the working directory, so
# a relative --bin-dir never becomes a relative ProgramArguments.
abs_bin_dir() {
    case "$BIN_DIR" in
    /*)
        printf '%s\n' "$BIN_DIR"
        ;;
    *)
        printf '%s/%s\n' "$(pwd)" "$BIN_DIR"
        ;;
    esac
}

# Escape the five XML special characters, so any path value is safe in
# the plist.
xml_escape() {
    printf '%s' "$1" | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g' -e 's/"/\&quot;/g' -e "s/'/\&apos;/g"
}

# Append an entry to the service PATH, in order and without
# duplication.
A2A_PATH_ENTRIES=""
a2a_add_path_entry() {
    [ -n "$1" ] || return 0
    case ":$A2A_PATH_ENTRIES:" in
    *":$1:"*) ;;
    *) A2A_PATH_ENTRIES="${A2A_PATH_ENTRIES:+$A2A_PATH_ENTRIES:}$1" ;;
    esac
}

# Build the service PATH: the bin directory, the soho and herdr
# directories (resolved on the PATH at install time) and the system
# directories, in that order and without duplication. A missing soho
# or herdr only warns: the serve runs, its notices do not.
a2a_build_service_path() {
    A2A_PATH_ENTRIES=""
    a2a_add_path_entry "$ABS_BIN_DIR"
    if A2A_SOHO_BIN="$(command -v soho)"; then
        a2a_add_path_entry "$(dirname -- "$A2A_SOHO_BIN")"
    else
        echo "nau install: warning: soho not found on the PATH; the a2a serve notices will not be sent" >&2
    fi
    if A2A_HERDR_BIN="$(command -v herdr)"; then
        a2a_add_path_entry "$(dirname -- "$A2A_HERDR_BIN")"
    else
        echo "nau install: warning: herdr not found on the PATH; the a2a serve notices will not be sent" >&2
    fi
    a2a_add_path_entry /usr/bin
    a2a_add_path_entry /bin
    a2a_add_path_entry /usr/sbin
    a2a_add_path_entry /sbin
}

# Print the LaunchAgent plist to stdout: nothing is written or started
# (this is also the --serve-dry-run output).
a2a_print_plist() {
    a2a_build_service_path
    esc_label="$(xml_escape "$A2A_LABEL")"
    esc_nau="$(xml_escape "$ABS_BIN_DIR/nau")"
    esc_log="$(xml_escape "$A2A_LOG")"
    esc_path="$(xml_escape "$A2A_PATH_ENTRIES")"
    cat <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0">
<dict>
<key>Label</key>
<string>$esc_label</string>
<key>ProgramArguments</key>
<array>
<string>$esc_nau</string>
<string>a2a</string>
<string>serve</string>
</array>
<key>RunAtLoad</key>
<true/>
<key>KeepAlive</key>
<true/>
<key>ThrottleInterval</key>
<integer>10</integer>
<key>StandardOutPath</key>
<string>$esc_log</string>
<key>StandardErrorPath</key>
<string>$esc_log</string>
<key>EnvironmentVariables</key>
<dict>
<key>PATH</key>
<string>$esc_path</string>
</dict>
</dict>
</plist>
EOF
}

# Wait for the serve to answer the card probe under a wall deadline
# of 20 s: each probe runs in the background and a watchdog (also
# ours) kills it when the remaining budget runs out, so a blocking
# probe cannot stretch the install past the deadline; the probe and
# the watchdog are waited for in every case. There is no GNU timeout
# on macOS, so the budget is enforced with the background kill. The
# watchdog keeps its own sleep's PID and, on signal, kills and waits
# that sleep before exiting, so no stray sleep survives the kill.
a2a_health_check() {
    deadline=$(( $(date +%s) + 20 ))
    while :; do
        now="$(date +%s)"
        [ "$now" -lt "$deadline" ] || break
        budget=$(( deadline - now ))
        "$NAU" a2a card --server "http://127.0.0.1:7430" >/dev/null 2>&1 &
        probe=$!
        (
            wd_sleep=""
            wd_stop() {
                if [ -n "$wd_sleep" ]; then
                    kill "$wd_sleep" 2>/dev/null || :
                    wait "$wd_sleep" 2>/dev/null || :
                fi
                exit 0
            }
            trap wd_stop TERM INT
            sleep "$budget" &
            wd_sleep=$!
            wait "$wd_sleep"
            kill "$probe" 2>/dev/null
        ) &
        watchdog=$!
        rc=0
        wait "$probe" || rc=$?
        kill "$watchdog" 2>/dev/null || :
        wait "$watchdog" 2>/dev/null || :
        if [ "$rc" -eq 0 ]; then
            echo "nau install: a2a serve running" >&2
            return 0
        fi
        [ "$(date +%s)" -lt "$deadline" ] || break
        sleep 0.5
    done
    echo "nau install: a2a serve did not answer on 127.0.0.1:7430; see $A2A_LOG" >&2
    exit 1
}

# (Re)start the a2a serve LaunchAgent and wait for its health answer.
# $1 is 1 when the plist was rewritten with different content: a
# loaded job is then bootout and bootstrapped, otherwise it is
# kickstarted; an unloaded job is bootstrapped.
a2a_restart_service() {
    changed="$1"
    uid="$(id -u)"
    loaded=0
    if launchctl print "gui/$uid/$A2A_LABEL" >/dev/null 2>&1; then
        loaded=1
    fi
    if [ "$loaded" -eq 1 ] && [ "$changed" -eq 1 ]; then
        launchctl bootout "gui/$uid/$A2A_LABEL"
        launchctl bootstrap "gui/$uid" "$A2A_PLIST"
    elif [ "$loaded" -eq 1 ]; then
        launchctl kickstart -k "gui/$uid/$A2A_LABEL"
    else
        launchctl bootstrap "gui/$uid" "$A2A_PLIST"
    fi
    a2a_health_check
}

# Write the plist (temp file + rename, mode 0600), create the log
# directory and (re)start the service.
a2a_install_service() {
    mkdir -p "$(dirname -- "$A2A_PLIST")"
    mkdir -p "$A2A_LOG_DIR"
    STAGE="$A2A_PLIST.tmp.$$"
    a2a_print_plist > "$STAGE"
    chmod 600 "$STAGE"
    changed=1
    if [ -f "$A2A_PLIST" ] && cmp -s "$STAGE" "$A2A_PLIST"; then
        changed=0
    fi
    mv "$STAGE" "$A2A_PLIST"
    STAGE=""
    a2a_restart_service "$changed"
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
    --serve)
        SERVE=1
        shift
        ;;
    --serve-dry-run)
        SERVE_DRY_RUN=1
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

OS="$(uname -s)"

# The service options are refused before any step (before the go
# check, before the install), on every non-macOS platform; on a
# platform without the plist, the automatic restart never happens.
if { [ "$SERVE" -eq 1 ] || [ "$SERVE_DRY_RUN" -eq 1 ]; } && [ "$OS" != "Darwin" ]; then
    echo "nau install: --serve is supported on macOS and Windows only" >&2
    exit 2
fi

# --serve-dry-run prints the plist and exits 0 before the go install,
# without touching anything (it wins over --serve when both are given).
if [ "$SERVE_DRY_RUN" -eq 1 ]; then
    ABS_BIN_DIR="$(abs_bin_dir)"
    a2a_print_plist
    exit 0
fi

if ! command -v go >/dev/null 2>&1; then
    echo "nau install: go not found on the PATH; install Go 1.26 or newer first" >&2
    exit 1
fi

# A dedicated temp dir, removed on any exit: the fresh binary is built
# there before the installed one is touched.
TMP="$(mktemp -d)"
STAGE=""
trap 'rm -rf "$TMP"; if [ -n "$STAGE" ]; then rm -f "$STAGE"; fi' EXIT

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
STAGE=""
NAU="$BIN_DIR/nau"

# a2a serve service (macOS only): with --serve, install (or rewrite)
# the LaunchAgent and (re)start it; without --serve, restart it when
# it is already installed (the plist exists), so an update never
# leaves the old binary running. This only happens after a successful
# swap: a failed go install never touches the service.
if [ "$OS" = "Darwin" ] && { [ "$SERVE" -eq 1 ] || [ -f "$A2A_PLIST" ]; }; then
    ABS_BIN_DIR="$(abs_bin_dir)"
    if [ "$SERVE" -eq 1 ]; then
        a2a_install_service
    else
        a2a_restart_service 0
    fi
fi

# Set machine_label only when it is still empty; never print the value.
if [ -n "$MACHINE_LABEL" ]; then
    CURRENT="$("$NAU" config get machine_label 2>/dev/null)" || CURRENT=""
    CURRENT_VALUE="$(printf '%s\n' "$CURRENT" | sed -n 's/.*"value":"\([^"]*\)".*/\1/p')"
    if [ -z "$CURRENT_VALUE" ]; then
        "$NAU" config set machine_label "$MACHINE_LABEL" >/dev/null
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
            KIND="$(printf '%s\n' "$LIST" | sed -n 's/.*"source":{[^}]*"kind":"\([^"]*\)".*/\1/p')"
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
