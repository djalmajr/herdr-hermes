# Install or update the nau binary and the optional Herdr plugin on
# Windows. Re-running the script with another -Ref updates the node to
# that ref, binary and plugin on the same ref.
#
# On Windows, -Serve installs (or rewrites) the a2a serve scheduled
# task and (re)starts it, and a re-run restarts the installed service
# after the binary swap, so the node never keeps running an old
# binary.
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
    [switch]$Serve,
    [switch]$ServeDryRun,
    [switch]$Help
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$PluginID = 'djalmajr.herdr-hermes'
$PluginSource = 'djalmajr/nau/plugin'
$ModulePath = 'github.com/djalmajr/nau/cmd/nau'
# The a2a serve user service: a scheduled task in the interactive user
# session, at the root task path. It is never a child of this script:
# an ssh session is session 0 and dies with the connection.
$A2aTaskName = 'nau a2a serve'
$A2aTaskPath = '\'
$A2aPortHost = '127.0.0.1'
$A2aPort = 7430
$A2aServerUrl = 'http://127.0.0.1:7430'

function Show-Usage {
    @'
Usage: powershell -File scripts\install.ps1 [-Ref <ref>] [-BinDir <dir>] [-MachineLabel <label>] [-NoPlugin] [-Serve] [-ServeDryRun]

Install or update the nau binary into the bin directory and the Herdr
plugin, both from the same ref. Updating is re-running the script with
another -Ref. On Windows, -Serve installs the a2a serve scheduled task
and (re)starts it; every re-run restarts the installed service after
the binary swap, so the node never keeps running an old binary.

Options:
  -Ref <ref>           ref to install (default: main)
  -BinDir <dir>        bin directory (default: %USERPROFILE%\.local\bin)
  -MachineLabel <lbl>  set machine_label when it is still empty
  -NoPlugin            skip the Herdr plugin step
  -Serve               install or rewrite the a2a serve scheduled task and (re)start it
  -ServeDryRun         print what -Serve would install (executable, arguments, triggers, settings, one per line) and exit without touching anything
  -Help                show this help
'@
}

function Fail([string]$Message) {
    Write-Host "nau install: $Message"
    exit 1
}

# Windows PowerShell 5.1 turns the redirected stderr of a native command
# into a terminating error under 'Stop'. Run a native command whose
# stderr is discarded under 'Continue' and judge it by its exit code.
function Invoke-Quiet([string]$FilePath, [string[]]$Arguments) {
    $ErrorActionPreference = 'Continue'
    & $FilePath @Arguments 2>$null
}

function Get-A2aLogPath {
    return (Join-Path $env:LOCALAPPDATA 'nau\a2a-serve.log')
}

function Get-A2aWrapperPath {
    return (Join-Path $env:LOCALAPPDATA 'nau\a2a-serve.cmd')
}

# The two wrapper lines with the expanded paths: conhost --headless
# re-tokenizes the client line and breaks nested quotes, so the task
# action runs this .cmd instead of quoting the paths itself.
function Get-A2aWrapperLines {
    $nau = Join-Path $BinDir 'nau.exe'
    $log = Get-A2aLogPath
    return @('@echo off', ('"' + $nau + '" a2a serve 1>>"' + $log + '" 2>&1'))
}

# The conhost action arguments: the wrapper path, expanded, is the
# only quoted argument, so a path with spaces works.
function Get-A2aActionArguments {
    return ('--headless cmd.exe /d /c "' + (Get-A2aWrapperPath) + '"')
}

# Write the wrapper .cmd atomically (temp file in the same folder, then
# Replace over the existing wrapper or Move on first creation) in
# ASCII with CRLF; on failure the stage is dropped and the previous
# wrapper is kept.
function Write-A2aWrapper {
    $wrapper = Get-A2aWrapperPath
    $dir = [System.IO.Path]::GetDirectoryName($wrapper)
    if (-not (Test-Path -LiteralPath $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
    $stage = Join-Path $dir ('.a2a-serve.cmd.' + $PID + '.tmp')
    $text = ((Get-A2aWrapperLines) -join "`r`n") + "`r`n"
    [System.IO.File]::WriteAllText($stage, $text, (New-Object System.Text.ASCIIEncoding))
    try {
        if (Test-Path -LiteralPath $wrapper) {
            # [NullString]::Value, not $null: PowerShell turns $null into an
            # empty string for a .NET string parameter, and Replace refuses it.
            [System.IO.File]::Replace($stage, $wrapper, [NullString]::Value)
        }
        else {
            [System.IO.File]::Move($stage, $wrapper)
        }
    }
    catch {
        Remove-Item -LiteralPath $stage -ErrorAction SilentlyContinue
        Fail 'could not install the a2a serve wrapper; the previous wrapper was kept'
    }
}

# Refuse (exit 2) when -Serve would have to store a character above
# 0x7F in the ASCII wrapper (it would be written as '?', pointing the
# task at another path): a non-ASCII character in the bin dir or in
# LOCALAPPDATA.
function Test-A2aNonAscii {
    foreach ($value in @($BinDir, $env:LOCALAPPDATA)) {
        foreach ($char in $value.ToCharArray()) {
            if ([int]$char -gt 0x7F) {
                Write-Host 'nau install: -Serve is refused: the bin dir or LOCALAPPDATA contains a non-ASCII character, which the service wrapper cannot hold'
                exit 2
            }
        }
    }
}

# Refuse (exit 2) when -Serve cannot build a safe cmd.exe command
# line: a double quote or a percent sign in the bin dir or in
# LOCALAPPDATA.
function Test-A2aQuotable {
    $bad = @()
    if ($BinDir.IndexOf('"') -ge 0 -or $BinDir.IndexOf('%') -ge 0) {
        $bad += 'the bin dir'
    }
    if ($env:LOCALAPPDATA.IndexOf('"') -ge 0 -or $env:LOCALAPPDATA.IndexOf('%') -ge 0) {
        $bad += 'the LOCALAPPDATA path'
    }
    if ($bad.Count -gt 0) {
        Write-Host ('nau install: -Serve is refused: ' + ($bad -join ' and ') + ' contains a double quote or a percent sign, which cmd.exe cannot quote safely')
        exit 2
    }
}

# Print what -Serve would install, one item per line on stdout, and
# touch nothing (this is also the -ServeDryRun output): the wrapper
# content and the action, the triggers and the settings.
function Show-A2aDryRun {
    'nau install: a2a serve dry run (nothing is installed or started)'
    'wrapper: ' + (Get-A2aWrapperPath)
    foreach ($line in (Get-A2aWrapperLines)) {
        'wrapper line: ' + $line
    }
    'executable: conhost.exe'
    'arguments: ' + (Get-A2aActionArguments)
    'trigger: at logon of the current user'
    'trigger: once, now, repeating every 5 minutes without a repetition duration'
    'settings: MultipleInstances=IgnoreNew'
    'settings: ExecutionTimeLimit=zero (no limit)'
    'settings: AllowStartIfOnBatteries'
    'settings: DontStopIfGoingOnBatteries'
}

# The connections listening on the a2a port, or the empty set.
function Get-A2aPortConnections {
    return @(Get-NetTCPConnection -LocalAddress $A2aPortHost -LocalPort $A2aPort -State Listen -ErrorAction SilentlyContinue)
}

# Stop the processes that listen on the a2a port, and only when their
# Path is in this bin dir and their name is nau.exe (or a
# nau.exe.old-*): the directory is compared literally (case-
# insensitively, so a BinDir with [ or ] cannot be read as a wildcard)
# and the pattern is applied to the file name only; a process that is
# not verifiable that way is refused (exit 1) and nothing is killed.
# No command line is ever read or printed: only the PID and the Path.
function Stop-A2aPortProcess {
    $ids = @{}
    foreach ($connection in (Get-A2aPortConnections)) {
        $ids[[int]$connection.OwningProcess] = $true
    }
    $ours = @{}
    foreach ($id in $ids.Keys) {
        $proc = Get-Process -Id $id -ErrorAction SilentlyContinue
        if (-not $proc) {
            continue
        }
        # Windows PowerShell 5.1 cannot resolve Split-Path -LiteralPath with
        # -Parent or -Leaf (AmbiguousParameterSet); the .NET calls take the
        # path literally on both runtimes.
        $dir = [System.IO.Path]::GetDirectoryName($proc.Path)
        $name = [System.IO.Path]::GetFileName($proc.Path)
        if ($dir -ieq $BinDir.TrimEnd([char]92) -and ($name -ieq 'nau.exe' -or $name -like 'nau.exe.old-*')) {
            $ours[$id] = $true
            continue
        }
        Write-Host 'nau install: 127.0.0.1:7430 is held by another program; the a2a serve was not restarted'
        exit 1
    }
    foreach ($id in $ours.Keys) {
        Stop-Process -Id $id -Force -ErrorAction SilentlyContinue
    }
}

# Wait, up to 10 s, for the a2a port to be free.
function Wait-A2aPortFree {
    $deadline = (Get-Date).AddSeconds(10)
    while ((Get-Date) -lt $deadline -and (Get-A2aPortConnections).Count -gt 0) {
        Start-Sleep -Milliseconds 500
    }
}

# Wait for the serve to answer the card probe under a wall deadline
# of 20 s: each probe runs in the background and is limited to the
# remaining budget; a probe still running when the deadline passes is
# killed (it is ours, by the PID kept here) and waited for.
function Test-A2aHealth {
    $deadline = (Get-Date).AddSeconds(20)
    while ((Get-Date) -lt $deadline) {
        $psi = New-Object System.Diagnostics.ProcessStartInfo
        $psi.FileName = (Join-Path $BinDir 'nau.exe')
        $psi.Arguments = ('a2a card --server ' + $A2aServerUrl)
        $psi.UseShellExecute = $false
        $psi.RedirectStandardOutput = $true
        $psi.RedirectStandardError = $true
        $psi.CreateNoWindow = $true
        $probe = [System.Diagnostics.Process]::Start($psi)
        try {
            # Drain both streams in parallel from the start, so a
            # probe that writes more than a pipe buffer cannot block
            # on the write while we wait for it.
            $outRead = $probe.StandardOutput.ReadToEndAsync()
            $errRead = $probe.StandardError.ReadToEndAsync()
            while ((Get-Date) -lt $deadline -and -not $probe.WaitForExit(250)) {
                # keep waiting in slices of 250 ms
            }
            if (-not $probe.HasExited) {
                $probe.Kill()
                $probe.WaitForExit()
            }
            $outRead.Wait()
            $errRead.Wait()
            if ($probe.ExitCode -eq 0) {
                Write-Host 'nau install: a2a serve running'
                return
            }
        }
        finally {
            $probe.Dispose()
        }
        Start-Sleep -Milliseconds 500
    }
    Write-Host ('nau install: a2a serve did not answer on 127.0.0.1:7430; see ' + (Get-A2aLogPath))
    exit 1
}

# Register (or replace, with -Force) the a2a serve scheduled task:
# principal the current user (interactive, limited), a logon trigger
# and a once-now trigger that repeats every 5 minutes without end (so
# a dead serve is re-erected), and the conhost action that runs the
# wrapper .cmd.
function Register-A2aTask {
    $principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
    $triggers = @(
        (New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME)
        (New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes 5))
    )
    $settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Seconds 0) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
    $action = New-ScheduledTaskAction -Execute 'conhost.exe' -Argument (Get-A2aActionArguments)
    $task = New-ScheduledTask -Settings $settings -Trigger $triggers -Action $action -Principal $principal
    # The wrapper (and the log next to it) must exist before the task
    # runs.
    Write-A2aWrapper
    Register-ScheduledTask -TaskName $A2aTaskName -TaskPath $A2aTaskPath -InputObject $task -Force
}

# Restart the a2a serve service, in order: stop the task (which kills
# the conhost root only, a child can survive), stop the process that
# still listens on the port (refused when it is not this bin dir's
# nau), wait for the port (and fail when it is still held, so the
# health check cannot be answered by the old serve), start the task,
# wait for the health answer.
function Restart-A2aService {
    Stop-ScheduledTask -TaskName $A2aTaskName -TaskPath $A2aTaskPath -ErrorAction SilentlyContinue
    Stop-A2aPortProcess
    Wait-A2aPortFree
    if ((Get-A2aPortConnections).Count -gt 0) {
        Fail 'the previous a2a serve did not stop; 127.0.0.1:7430 is still held'
    }
    Start-ScheduledTask -TaskName $A2aTaskName -TaskPath $A2aTaskPath
    Test-A2aHealth
}

if ($Help) {
    Show-Usage
    exit 0
}

if ($BinDir -eq '') {
    $BinDir = Join-Path $env:USERPROFILE '.local\bin'
}

# Normalize once, right after the parameters are read and before the
# validations and the dry-run, and use that value everywhere: the
# task action and the process-owner check need an absolute path, and
# a relative one would not resolve against the task's working
# directory.
$BinDir = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($BinDir)

if ($Serve -or $ServeDryRun) {
    # Before anything else: an empty or undefined LOCALAPPDATA is
    # refused (exit 2) without raising an exception.
    if ([string]::IsNullOrEmpty($env:LOCALAPPDATA)) {
        Write-Host 'nau install: -Serve needs LOCALAPPDATA'
        exit 2
    }
    Test-A2aQuotable
    Test-A2aNonAscii
}

# -ServeDryRun prints the task definition and exits 0 before the go
# install, without touching anything (it wins over -Serve when both
# are given).
if ($ServeDryRun) {
    Show-A2aDryRun
    exit 0
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
    $null = Invoke-Quiet $NauNew @('version')
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
    $Stamp = (Get-Date -Format 'yyyyMMddHHmmss') + '-' + $PID
    # Stage the new binary next to the target first, on the same volume,
    # so the swap below is two renames in one directory.
    $NauStage = Join-Path $BinDir ('.nau.new-' + $Stamp + '.exe')
    $Backup = ''
    try {
        Copy-Item -LiteralPath $NauNew -Destination $NauStage
        if (Test-Path -LiteralPath $NauTarget) {
            $Backup = 'nau.exe.old-' + $Stamp
            Rename-Item -LiteralPath $NauTarget -NewName $Backup
        }
        Rename-Item -LiteralPath $NauStage -NewName 'nau.exe'
    }
    catch {
        # Put the previous binary back under its name, drop the stage
        # and stop: the node keeps the nau it had.
        if ($Backup -ne '' -and -not (Test-Path -LiteralPath $NauTarget)) {
            Rename-Item -LiteralPath (Join-Path $BinDir $Backup) -NewName 'nau.exe' -ErrorAction SilentlyContinue
        }
        Remove-Item -LiteralPath $NauStage -ErrorAction SilentlyContinue
        Fail 'could not replace the installed binary; the previous one was kept'
    }

    # a2a serve service: -Serve registers (or replaces) the task and
    # restarts it; without -Serve, restart it when it is already
    # registered, so an update never leaves the old binary running.
    # This only happens after a successful swap: a failed go install
    # never touches the service.
    $existingA2aTask = $null
    if (-not $Serve) {
        $existingA2aTask = Get-ScheduledTask -TaskName $A2aTaskName -TaskPath $A2aTaskPath -ErrorAction SilentlyContinue
    }
    if ($Serve -or $existingA2aTask) {
        if ($Serve) {
            Register-A2aTask
        }
        Restart-A2aService
    }

    # Set machine_label only when it is still empty; never print the value.
    if ($MachineLabel -ne '') {
        $current = Invoke-Quiet $NauTarget @('config', 'get', 'machine_label')
        if ($LASTEXITCODE -ne 0) {
            $current = ''
        }
        $currentValue = ''
        $match = [regex]::Match(($current -join "`n"), '"value":"([^"]*)"')
        if ($match.Success) {
            $currentValue = $match.Groups[1].Value
        }
        if ($currentValue -eq '') {
            $null = & $NauTarget config set machine_label $MachineLabel
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
            $list = Invoke-Quiet 'herdr' @('plugin', 'list', '--plugin', $PluginID, '--json')
            if ($LASTEXITCODE -ne 0) {
                Fail ('herdr plugin list failed (exit ' + $LASTEXITCODE + ')')
            }
            $listText = $list -join "`n"
            if ($listText.Contains('"plugin_id":"djalmajr.herdr-hermes"')) {
                # Installed: a local origin (herdr plugin link) is
                # refused, a GitHub origin is updated in place.
                $match = [regex]::Match($listText, '"source":\{[^}]*"kind":"([^"]*)"')
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
    # Remove the old binaries and leftover stages that are not in use,
    # after the a2a serve restart, because the old serve is what holds
    # a .old file open; the ones still in use cannot be removed and are
    # silently ignored.
    if (Test-Path -LiteralPath $BinDir) {
        Get-ChildItem -LiteralPath $BinDir -File | Where-Object { $_.Name -like 'nau.exe.old-*' -or $_.Name -like '.nau.new-*.exe' } | ForEach-Object {
            try {
                Remove-Item -LiteralPath $_.FullName -ErrorAction Stop
            }
            catch {
                # In use; leave it for the next run.
            }
        }
    }
    # Remove wrapper stages that are not in use (a crash between the
    # write and the Replace/Move); the ones still in use cannot be
    # removed and are silently ignored.
    if (-not [string]::IsNullOrEmpty($env:LOCALAPPDATA)) {
        $a2aDir = Join-Path $env:LOCALAPPDATA 'nau'
        if (Test-Path -LiteralPath $a2aDir) {
            Get-ChildItem -LiteralPath $a2aDir -File -Filter '.a2a-serve.cmd.*.tmp' | ForEach-Object {
                try {
                    Remove-Item -LiteralPath $_.FullName -ErrorAction Stop
                }
                catch {
                    # In use; leave it for the next run.
                }
            }
        }
    }
    # A fresh exe can stay locked for a moment after it ran (an antivirus
    # scan, for example): retry the removal briefly before giving up.
    for ($i = 0; $i -lt 5 -and (Test-Path -LiteralPath $Tmp); $i++) {
        Remove-Item -LiteralPath $Tmp -Recurse -Force -ErrorAction SilentlyContinue
        if (Test-Path -LiteralPath $Tmp) {
            Start-Sleep -Milliseconds 500
        }
    }
}
