// Package scripts_test checks the install scripts hermetically: the
// POSIX shell syntax, the --help output and the --serve-dry-run output
// of install.sh (a LaunchAgent plist on macOS, refused on Linux), and
// the PowerShell syntax, the -Help output and the -ServeDryRun output
// of install.ps1 (when a PowerShell is available). The checks parse,
// print usage or print the service definition only: no install, no
// service install or restart, no network and no Herdr state change.
package scripts_test

import (
	"bytes"
	"errors"
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

// shAvailable returns the path of sh, skipping the test on Windows or
// when sh is not on the PATH.
func shAvailable(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("sh is not available on windows")
	}
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("sh not available: " + err.Error())
	}
	return sh
}

// powershellAvailable returns the name of the first available
// PowerShell (pwsh or powershell), skipping the test when neither is
// on the PATH.
func powershellAvailable(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"pwsh", "powershell"} {
		if _, err := exec.LookPath(name); err == nil {
			return name
		}
	}
	t.Skip("neither pwsh nor powershell is available")
	return ""
}

func TestInstallShSyntax(t *testing.T) {
	sh := shAvailable(t)
	out, err := exec.Command(sh, "-n", "install.sh").CombinedOutput()
	if err != nil {
		t.Fatalf("sh -n install.sh: %v\n%s", err, out)
	}
}

func TestInstallShHelp(t *testing.T) {
	sh := shAvailable(t)
	out, err := exec.Command(sh, "install.sh", "--help").CombinedOutput()
	if err != nil {
		t.Fatalf("sh install.sh --help: %v\n%s", sh, out)
	}
	text := string(out)
	for _, opt := range []string{"--ref", "--bin-dir", "--machine-label", "--no-plugin", "--serve", "--serve-dry-run", "--help"} {
		if !strings.Contains(text, opt) {
			t.Errorf("install.sh --help does not list %q:\n%s", opt, text)
		}
	}
}

// TestInstallShServeDryRun runs --serve-dry-run: on macOS it must exit
// 0 and print a plist that passes plutil -lint (skipped when plutil
// is absent); on Linux it must be refused with exit 2 before any step.
func TestInstallShServeDryRun(t *testing.T) {
	sh := shAvailable(t)
	if runtime.GOOS == "linux" {
		cmd := exec.Command(sh, "install.sh", "--serve-dry-run")
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("sh install.sh --serve-dry-run: unexpected error: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
		}
		if exitErr.ExitCode() != 2 {
			t.Fatalf("sh install.sh --serve-dry-run: exit %d, want 2\nstdout:\n%s\nstderr:\n%s", exitErr.ExitCode(), stdout.String(), stderr.String())
		}
		return
	}
	if runtime.GOOS != "darwin" {
		t.Skipf("--serve-dry-run is only defined on macOS (GOOS %s)", runtime.GOOS)
	}
	cmd := exec.Command(sh, "install.sh", "--serve-dry-run")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("sh install.sh --serve-dry-run: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	plutil, err := exec.LookPath("plutil")
	if err != nil {
		t.Skip("plutil not available: " + err.Error())
	}
	lint := exec.Command(plutil, "-lint", "-")
	lint.Stdin = strings.NewReader(stdout.String())
	lintOut, err := lint.CombinedOutput()
	if err != nil {
		t.Fatalf("plutil -lint - on the dry-run plist: %v\n%s\nplist:\n%s", err, lintOut, stdout.String())
	}
}

func TestInstallPs1Parses(t *testing.T) {
	powershell := powershellAvailable(t)
	// Parse the script with the PowerShell parser and fail when the
	// parser reports any error.
	script := strings.Join([]string{
		"$errors = $null",
		"[void][System.Management.Automation.Language.Parser]::ParseFile((Join-Path (Get-Location).Path 'install.ps1'), [ref]$null, [ref]$errors)",
		"if ($errors.Count -gt 0) {",
		"    foreach ($e in $errors) { Write-Output $e }",
		"    exit 1",
		"}",
	}, "\n")
	out, err := exec.Command(powershell, "-NoProfile", "-NonInteractive", "-Command", script).CombinedOutput()
	if err != nil {
		t.Fatalf("%s parse install.ps1: %v\n%s", powershell, err, out)
	}
}

func TestInstallPs1Help(t *testing.T) {
	powershell := powershellAvailable(t)
	out, err := exec.Command(powershell, "-NoProfile", "-NonInteractive", "-File", "install.ps1", "-Help").CombinedOutput()
	if err != nil {
		t.Fatalf("%s install.ps1 -Help: %v\n%s", powershell, err, out)
	}
	text := string(out)
	for _, opt := range []string{"-Ref", "-BinDir", "-MachineLabel", "-NoPlugin", "-Serve", "-ServeDryRun", "-Help"} {
		if !strings.Contains(text, opt) {
			t.Errorf("install.ps1 -Help does not list %q:\n%s", opt, text)
		}
	}
}

// TestInstallPs1ServeDryRun runs -ServeDryRun on Windows: it must exit
// 0 and print the action executable and arguments, the triggers and
// the settings (one per line). Skipped off Windows.
func TestInstallPs1ServeDryRun(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("install.ps1 -ServeDryRun is only exercised on windows")
	}
	powershell := powershellAvailable(t)
	out, err := exec.Command(powershell, "-NoProfile", "-NonInteractive", "-File", "install.ps1", "-ServeDryRun").CombinedOutput()
	if err != nil {
		t.Fatalf("%s install.ps1 -ServeDryRun: %v\n%s", powershell, err, out)
	}
	text := string(out)
	for _, want := range []string{"a2a serve", "conhost.exe", "IgnoreNew"} {
		if !strings.Contains(text, want) {
			t.Errorf("install.ps1 -ServeDryRun output does not contain %q:\n%s", want, text)
		}
	}
}
