param(
    [Parameter(Mandatory = $true)]
    [string]$T3Command,
    [string]$TaskName = "T3 Code Headless",
    [int]$Port = 3773
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $T3Command -PathType Leaf)) {
    throw "T3 command does not exist"
}
if ($Port -lt 1 -or $Port -gt 65535) {
    throw "Port must be between 1 and 65535"
}

$EscapedCommand = $T3Command -replace '"', '""'
$TaskArguments = (
    '/d /s /c ""{0}" serve --no-browser --host 127.0.0.1 --port {1}"'
) -f $EscapedCommand, $Port
$Action = New-ScheduledTaskAction `
    -Execute $env:ComSpec `
    -Argument $TaskArguments
$Trigger = New-ScheduledTaskTrigger -AtLogOn

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -RunLevel Limited `
    -Force | Out-Null
Start-ScheduledTask -TaskName $TaskName

$Deadline = [DateTime]::UtcNow.AddSeconds(30)
$Ready = $false
while ([DateTime]::UtcNow -lt $Deadline) {
    try {
        $Response = Invoke-WebRequest `
            -UseBasicParsing `
            -Uri "http://127.0.0.1:$Port/.well-known/t3/environment" `
            -TimeoutSec 2
        if ($Response.StatusCode -eq 200) {
            $Ready = $true
            break
        }
    }
    catch {
        Start-Sleep -Milliseconds 500
    }
}

if (-not $Ready) {
    throw "T3 scheduled task did not become ready"
}

[pscustomobject]@{
    task = $TaskName
    state = "ready"
    port = $Port
} | ConvertTo-Json -Compress
