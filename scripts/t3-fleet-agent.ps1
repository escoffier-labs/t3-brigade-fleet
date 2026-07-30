param()

$ErrorActionPreference = "Stop"
$ErrorPayloads = @{
    "invalid_config" = '{"schema":"t3-fleet.agent-error.v1","error":{"code":"invalid_config","message":"target configuration is invalid"}}'
    "invalid_request" = '{"schema":"t3-fleet.agent-error.v1","error":{"code":"invalid_request","message":"target request is invalid"}}'
    "internal_error" = '{"schema":"t3-fleet.agent-error.v1","error":{"code":"internal_error","message":"internal target error"}}'
}

function Write-AgentError {
    param([string]$Code)

    [Console]::Out.WriteLine($ErrorPayloads[$Code])
    [Console]::Error.WriteLine("t3-fleet-agent: $Code")
}

if ($args.Count -ne 0) {
    Write-AgentError "invalid_request"
    exit 2
}

$PythonExecutable = $env:T3_FLEET_PYTHON
if ([string]::IsNullOrWhiteSpace($PythonExecutable)) {
    Write-AgentError "invalid_config"
    exit 2
}

try {
    $SavedErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $AgentOutput = @(& $PythonExecutable -m t3_fleet.agent 2>$null)
    $AgentExitCode = $LASTEXITCODE
    $ErrorActionPreference = $SavedErrorActionPreference
} catch {
    $ErrorActionPreference = $SavedErrorActionPreference
    Write-AgentError "internal_error"
    exit 1
}

if ($AgentOutput.Count -ne 1) {
    Write-AgentError "internal_error"
    exit 1
}

$AgentJson = [string]$AgentOutput[0]
try {
    $AgentResult = $AgentJson | ConvertFrom-Json
} catch {
    Write-AgentError "internal_error"
    exit 1
}

if ($AgentExitCode -eq 0 -and $AgentResult.schema -eq "t3-fleet.agent-result.v1") {
    [Console]::Out.WriteLine($AgentJson)
    exit 0
}

$AgentErrorCode = [string]$AgentResult.error.code
if (
    $AgentExitCode -ne 0 -and
    $AgentResult.schema -eq "t3-fleet.agent-error.v1" -and
    $AgentErrorCode -in @(
        "invalid_config",
        "invalid_request",
        "host_unavailable",
        "idempotency_conflict",
        "unknown_state",
        "t3_auth_failed",
        "t3_http_failed",
        "internal_error"
    )
) {
    [Console]::Out.WriteLine($AgentJson)
    [Console]::Error.WriteLine("t3-fleet-agent: $AgentErrorCode")
    exit $AgentExitCode
}

Write-AgentError "internal_error"
exit 1
