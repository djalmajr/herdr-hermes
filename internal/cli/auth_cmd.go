package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode"
)

// keyStdinCap is the maximum size of a key read from stdin: 4 KiB.
const keyStdinCap = 4 * 1024

var (
	errInputOverCap  = errors.New("input over the size cap")
	errKeyEmpty      = errors.New("the key is empty")
	errKeyWhitespace = errors.New("the key must not contain whitespace")
	errKeyEchoOff    = errors.New("could not disable terminal echo; refusing to read the key with echo on")
)

// cmdAuth implements `auth login --key -`, `auth status` and `auth logout`.
// The key is read only from stdin (pipe, file or an interactive terminal
// with echo off); it is never accepted as a flag value and no environment
// variable is ever read for it.
func cmdAuth(args []string, env Env) int {
	if len(args) == 0 {
		badUsage(env, "usage: auth login --key - | auth status | auth logout")
		return 2
	}
	switch args[0] {
	case "status":
		return authStatus(args, env)
	case "login":
		return authLogin(args, env)
	case "logout":
		return authLogout(args, env)
	default:
		badUsage(env, "unknown auth subcommand "+quote(args[0]))
		return 2
	}
}

// authStatus prints whether a key is configured. It works under
// HERDR_HERMES_NOWRITE=1 and creates nothing.
func authStatus(args []string, env Env) int {
	if len(args) != 1 {
		badUsage(env, "usage: auth status")
		return 2
	}
	_, _ = fmt.Fprintf(env.Stdout, "{\"configured\":%t,\"store\":\"file\"}\n", keyConfigured(env))
	return 0
}

// authLogin stores the key read from stdin. `auth login --key -` is the only
// accepted form; any other argument shape exits 2 with a message that never
// echoes a value.
func authLogin(args []string, env Env) int {
	if env.Getenv(nowriteVar) == "1" {
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", nowriteErrorJSON)
		return 2
	}
	if len(args) != 3 || args[1] != "--key" || args[2] != "-" {
		badUsage(env, "auth login: the only accepted form is `auth login --key -`; the key is read from stdin, never taken as a flag value or from the environment")
		return 2
	}
	key, err := readKey(env)
	if errors.Is(err, errInputOverCap) {
		fail(env, 2, "auth login: the key is over 4 KiB")
		return 2
	}
	if err != nil {
		fail(env, 2, "auth login: "+err.Error())
		return 2
	}
	if err := newCredStore(env).Set(key); err != nil {
		fail(env, 2, "auth login: "+err.Error())
		return 2
	}
	_, _ = fmt.Fprintf(env.Stdout, "{\"configured\":true,\"store\":\"file\"}\n")
	return 0
}

// authLogout removes the stored key.
func authLogout(args []string, env Env) int {
	if env.Getenv(nowriteVar) == "1" {
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", nowriteErrorJSON)
		return 2
	}
	if len(args) != 1 {
		badUsage(env, "usage: auth logout")
		return 2
	}
	if err := newCredStore(env).Delete(); err != nil {
		fail(env, 2, "auth logout: "+err.Error())
		return 2
	}
	_, _ = fmt.Fprintf(env.Stdout, "{\"configured\":false,\"store\":\"file\"}\n")
	return 0
}

// readKey reads the key. A terminal stdin gets the interactive prompt with
// echo disabled; anything else (a pipe or a file) is read directly. The key
// is at most 4 KiB, and one trailing newline (LF or CRLF) is trimmed.
func readKey(env Env) (string, error) {
	if f, ok := env.Stdin.(*os.File); ok {
		if st, err := f.Stat(); err == nil && st.Mode()&os.ModeCharDevice != 0 {
			return readKeyTerminal(env, f)
		}
	}
	data, err := readCapped(env.Stdin, keyStdinCap)
	if err != nil {
		return "", err
	}
	return validateKey(data)
}

// readKeyTerminal prompts on stderr and reads the key line with echo
// disabled; the echo flag is restored on every path (defer). When echo
// cannot be disabled the key is not read at all. The read stops at the
// newline (a terminal has no EOF) or at the 4 KiB cap.
func readKeyTerminal(env Env, f *os.File) (string, error) {
	_, _ = io.WriteString(env.Stderr, "API key: ")
	if err := setTermEcho(f, false); err != nil {
		return "", errKeyEchoOff
	}
	defer func() { _ = setTermEcho(f, true) }()
	line, err := readLine(f, keyStdinCap)
	if err != nil {
		return "", err
	}
	return validateKey(line)
}

// readLine reads one line (up to the newline) from r, at most cap bytes.
// A line longer than cap is errInputOverCap; a zero-byte read with an error
// returns what was read so far. Chunks are accumulated because a canonical
// terminal delivers a whole line per read while a raw one may deliver
// partial lines.
func readLine(r io.Reader, cap int) ([]byte, error) {
	buf := make([]byte, cap+1)
	var line []byte
	for len(line) < cap+1 {
		n, err := r.Read(buf)
		if n > 0 {
			if i := bytes.IndexByte(buf[:n], '\n'); i >= 0 {
				line = append(line, buf[:i]...)
				if len(line) > cap {
					return line, errInputOverCap
				}
				return line, nil
			}
			line = append(line, buf[:n]...)
		}
		if err != nil {
			return line, err
		}
	}
	return line, errInputOverCap
}

// readCapped reads at most cap bytes from r (cap+1 to detect an overrun);
// an overrun is errInputOverCap.
func readCapped(r io.Reader, cap int) ([]byte, error) {
	buf := make([]byte, cap+1)
	n, err := io.ReadFull(r, buf)
	if n > cap {
		return nil, errInputOverCap
	}
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	return buf[:n], nil
}

// validateKey trims one trailing LF (or CRLF) and refuses an empty key or
// a key containing any whitespace.
func validateKey(data []byte) (string, error) {
	key := string(data)
	if strings.HasSuffix(key, "\n") {
		key = key[:len(key)-1]
		if strings.HasSuffix(key, "\r") {
			key = key[:len(key)-1]
		}
	}
	if key == "" {
		return "", errKeyEmpty
	}
	for _, r := range key {
		if unicode.IsSpace(r) {
			return "", errKeyWhitespace
		}
	}
	return key, nil
}
