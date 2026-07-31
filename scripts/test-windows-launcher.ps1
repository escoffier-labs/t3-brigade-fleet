param(
    [Parameter(Mandatory = $true)]
    [string]$Launcher,
    [Parameter(Mandatory = $true)]
    [string]$PythonExecutable
)

$ErrorActionPreference = "Stop"
$OriginalPython = $env:T3_FLEET_PYTHON
$ConflictChild = Join-Path ([System.IO.Path]::GetTempPath()) (
    "t3-fleet-conflict-{0}.cmd" -f [guid]::NewGuid().ToString("N")
)

function Invoke-LauncherCase {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [AllowNull()]
        [string]$ConfiguredPython,
        [string[]]$LauncherArguments = @(),
        [Parameter(Mandatory = $true)]
        [int]$ExpectedExit,
        [Parameter(Mandatory = $true)]
        [string]$ExpectedCode
    )

    $StdoutFile = [System.IO.Path]::GetTempFileName()
    $StderrFile = [System.IO.Path]::GetTempFileName()
    try {
        if ($null -eq $ConfiguredPython) {
            Remove-Item Env:T3_FLEET_PYTHON -ErrorAction SilentlyContinue
        }
        else {
            $env:T3_FLEET_PYTHON = $ConfiguredPython
        }

        $ArgumentsText = ($LauncherArguments | ForEach-Object {
            '"{0}"' -f ($_ -replace '"', '""')
        }) -join " "
        $CommandLine = (
            'powershell.exe -NoProfile -NonInteractive -File "{0}" {1} ' +
            '1>"{2}" 2>"{3}"'
        ) -f $Launcher, $ArgumentsText, $StdoutFile, $StderrFile
        & cmd.exe /d /s /c $CommandLine
        $ExitCode = $LASTEXITCODE
        $StdoutLines = @(Get-Content -LiteralPath $StdoutFile)
        $Stderr = Get-Content -Raw -LiteralPath $StderrFile

        if ($StdoutLines.Count -ne 1) {
            throw "$Name emitted $($StdoutLines.Count) stdout lines"
        }
        try {
            $Payload = $StdoutLines[0] | ConvertFrom-Json
        }
        catch {
            throw "$Name emitted invalid JSON"
        }
        if ($ExitCode -ne $ExpectedExit) {
            throw "$Name exited $ExitCode instead of $ExpectedExit"
        }
        if ($Payload.schema -ne "t3-fleet.agent-error.v1") {
            throw "$Name emitted the wrong schema"
        }
        if ($Payload.error.code -ne $ExpectedCode) {
            throw "$Name emitted error code $($Payload.error.code)"
        }
        if (
            $Stderr -match [regex]::Escape($env:USERPROFILE) -or
            $Stderr -match "T3_FLEET_PYTHON="
        ) {
            throw "$Name leaked a private path or environment value"
        }

        return [pscustomobject]@{
            name = $Name
            exit = $ExitCode
            stdout_lines = $StdoutLines.Count
            schema = $Payload.schema
            code = $Payload.error.code
            stderr_redacted = $true
        }
    }
    finally {
        Remove-Item -LiteralPath $StdoutFile, $StderrFile `
            -Force -ErrorAction SilentlyContinue
    }
}

try {
    @'
@echo off
echo {"schema":"t3-fleet.agent-error.v1","error":{"code":"idempotency_conflict","message":"target request conflicts with an existing request"}}
exit /b 4
'@ | Set-Content -LiteralPath $ConflictChild -Encoding ascii

    $Cases = @(
        Invoke-LauncherCase `
            -Name "missing-env" `
            -ConfiguredPython $null `
            -ExpectedExit 2 `
            -ExpectedCode "invalid_config"
        Invoke-LauncherCase `
            -Name "extra-argv" `
            -ConfiguredPython $PythonExecutable `
            -LauncherArguments @("unexpected") `
            -ExpectedExit 2 `
            -ExpectedCode "invalid_request"
        Invoke-LauncherCase `
            -Name "child-launch-failure" `
            -ConfiguredPython "C:\nonexistent\t3-fleet-python.exe" `
            -ExpectedExit 1 `
            -ExpectedCode "internal_error"
        Invoke-LauncherCase `
            -Name "child-error" `
            -ConfiguredPython $PythonExecutable `
            -ExpectedExit 2 `
            -ExpectedCode "invalid_request"
        Invoke-LauncherCase `
            -Name "idempotency-conflict" `
            -ConfiguredPython $ConflictChild `
            -ExpectedExit 4 `
            -ExpectedCode "idempotency_conflict"
    )
    $Cases | ConvertTo-Json -Compress
}
finally {
    Remove-Item -LiteralPath $ConflictChild -Force -ErrorAction SilentlyContinue
    if ($null -eq $OriginalPython) {
        Remove-Item Env:T3_FLEET_PYTHON -ErrorAction SilentlyContinue
    }
    else {
        $env:T3_FLEET_PYTHON = $OriginalPython
    }
}

exit 0
