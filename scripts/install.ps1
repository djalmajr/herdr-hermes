# Install or update the nau binary and the optional Herdr plugin on
# Windows. Re-running the script with another -Ref updates the node to
# that ref, binary and plugin on the same ref.
#
# Prerequisites: Go 1.26 or newer on the PATH; for the plugin step,
# Herdr on the PATH. The script never prints environment variables or
# configuration contents.

[CmdletBinding()]
param(
    [string]$Ref = 'main',
    [string]$BinDir = '',
    [string]$MachineLabel = '',
    [switch]$NoPlugin,
    [switch]$Help
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$PluginID = 'djalmajr.herdr-hermes'
$PluginSource = 'djalmajr/nau/plugin'
$ModulePath = 'github.com/djalmajr/nau/cmd/nau'

function Show-Usage {
    @"
Usage: powershell -File scripts\install.ps1 [-Ref <ref>] [-BinDir <dir>] [-MachineLabel <label>] [-NoPlugin]

Install or update the nau binary into the bin directory and the Herdr
plugin, both from the same ref. Updating is re-running the script with
another -Ref.

Options:
  -Ref <ref>           ref to install (default: main)
  -BinDir <dir>        bin directory (default: $env:USERPROFILE\.local\bin)
  -MachineLabel <lbl>  set machine_label when it is still empty
  -NoPlugin            skip the Herdr plugin step
  -Help                show this help
"@
}

function Fail([string]$Message) {
    Write-Host "nau install: $Message"
    exit 1
}

if ($Help) {
    Show-Usage
    exit 0
}

if ($BinDir -eq '') {
    $BinDir = Join-Path $env:USERPROFILE '.local\bin'
}

$Go = Get-Command go -ErrorAction SilentlyContinue
if (-not $Go) {
    Fail 'go not found on the PATH; install Go 1.26 or newer first'
}

# A dedicated temp dir, removed on any exit: the fresh binary is built
# there before the installed one is touched.
$Tmp = Join-Path $env:TEMP ('nau-install-' + $PID + '-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $Tmp | Out-Null
try {
    Write-Host "nau install: installing $ModulePath@$Ref"
    $env:GOBIN = $Tmp
    $env:GOPROXY = 'direct'
    go install "$ModulePath`@$Ref"
    if ($LASTEXITCODE -ne 0) {
        Fail ("go install failed (exit " + $LASTEXITCODE + ')')
    }

    # Test the new binary before touching the installed one.
    Write-Host 'nau install: checking the new binary'
    $NauNew = Join-Path $Tmp 'nau.exe'
    if (-not (Test-Path -LiteralPath $NauNew)) {
        Fail 'go install did not produce the new binary'
    }
    $null = & $NauNew version 2>$null
    if ($LASTEXITCODE -ne 0) {
        Fail 'the new binary failed ''nau version''; the installed binary is untouched'
    }

    # Replace the installed binary: Windows allows renaming an exe that
    # is in use, so move the old one aside first, then move the new one
    # into place.
    if (-not (Test-Path -LiteralPath $BinDir)) {
        New-Item -ItemType Directory -Path $BinDir -Force | Out-Null
    }
    $NauTarget = Join-Path $BinDir 'nau.exe'
    if (Test-Path -LiteralPath $NauTarget) {
        Rename-Item -LiteralPath $NauTarget -NewName ('nau.exe.old-' + (Get-Date -Format 'yyyyMMddHHmmss'))
    }
    Move-Item -LiteralPath $NauNew -Destination $NauTarget
    # Remove the old binaries that are not in use; the ones still in use
    # cannot be removed and are silently ignored.
    Get-ChildItem -LiteralPath $BinDir -Filter 'nau.exe.old-*' -File | ForEach-Object {
        try {
            Remove-Item -LiteralPath $_.FullName -ErrorAction Stop
        }
        catch {
            # In use; leave it for the next run.
        }
    }

    # Set machine_label only when it is still empty; never print the value.
    if ($MachineLabel -ne '') {
        $current = & $NauTarget config get machine_label 2>$null
        if ($LASTEXITCODE -ne 0) {
            $current = ''
        }
        $currentValue = ''
        $match = [regex]::Match(($current -join "`n"), '"value":"([^"]*)"')
        if ($match.Success) {
            $currentValue = $match.Groups[1].Value
        }
        if ($currentValue -eq '') {
            & $NauTarget config set machine_label $MachineLabel
            if ($LASTEXITCODE -ne 0) {
                Fail ("nau config set machine_label failed (exit " + $LASTEXITCODE + ')')
            }
            Write-Host 'nau install: machine_label set'
        }
        else {
            Write-Host 'nau install: machine_label is already set; keeping the current value'
        }
    }

    # The Herdr plugin, unless skipped or Herdr is absent.
    if ($NoPlugin) {
        Write-Host 'nau install: skipping the Herdr plugin (-NoPlugin)'
    }
    else {
        $herdr = Get-Command herdr -ErrorAction SilentlyContinue
        if (-not $herdr) {
            Write-Host 'nau install: herdr not found on the PATH; skipping the Herdr plugin'
        }
        else {
            $list = herdr plugin list --plugin $PluginID --json 2>$null
            if ($LASTEXITCODE -ne 0) {
                Fail ('herdr plugin list failed (exit ' + $LASTEXITCODE + ')')
            }
            $listText = $list -join "`n"
            if ($listText.Contains('"plugin_id":"djalmajr.herdr-hermes"')) {
                # Installed: a local origin (herdr plugin link) is
                # refused, a GitHub origin is updated in place.
                $match = [regex]::Match($listText, '"kind":"([^"]*)"')
                if (-not $match.Success) {
                    Fail ("could not determine the source of the installed herdr plugin " + $PluginID + "; if it is linked to a local path, run 'herdr plugin unlink " + $PluginID + "' first")
                }
                if ($match.Groups[1].Value -ne 'github') {
                    Fail ("the herdr plugin " + $PluginID + " is linked to a local path; run 'herdr plugin unlink " + $PluginID + "' first and re-run this script")
                }
            }
            herdr plugin install $PluginSource --ref $Ref -y
            if ($LASTEXITCODE -ne 0) {
                Fail ('herdr plugin install failed (exit ' + $LASTEXITCODE + ')')
            }
            Write-Host ("nau install: herdr plugin " + $PluginID + " installed from " + $PluginSource + ' (ref ' + $Ref + ')')
        }
    }

    # Warn, without failing, when the bin dir is not on the PATH.
    $inPath = $false
    foreach ($entry in ($env:Path -split ';')) {
        if ($entry -ne '' -and $entry -ieq $BinDir) {
            $inPath = $true
            break
        }
    }
    if (-not $inPath) {
        Write-Host ('nau install: warning: ' + $BinDir + ' is not on the PATH; add it to use nau and the plugin')
    }

    Write-Host 'nau install: installed'
    & $NauTarget version
    if ($LASTEXITCODE -ne 0) {
        Fail ('nau version failed (exit ' + $LASTEXITCODE + ')')
    }
    # doctor is informational: its non-zero exit (for example a missing
    # backend) must not fail the install.
    & $NauTarget doctor
    if ($LASTEXITCODE -ne 0) {
        Write-Host 'nau install: doctor reported a problem (informational); see the output above'
    }
}
finally {
    Remove-Item -Recurse -Force $Tmp -ErrorAction SilentlyContinue
}
