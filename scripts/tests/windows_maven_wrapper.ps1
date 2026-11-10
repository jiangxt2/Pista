param(
    [string]$Wrapper = (Join-Path $PSScriptRoot '..\..\mvnw.cmd')
)

$ErrorActionPreference = 'Stop'
$root = Join-Path ([System.IO.Path]::GetTempPath()) ('pista-wrapper-test-' + [guid]::NewGuid())
New-Item -ItemType Directory -Path $root | Out-Null

function New-TestArchive([string]$Name, [bool]$Modified) {
    $case = Join-Path $root $Name
    $distribution = Join-Path $case 'source\apache-maven-3.9.5'
    $bin = Join-Path $distribution 'bin'
    $repo = Join-Path $case 'repo'
    New-Item -ItemType Directory -Path $bin, $repo -Force | Out-Null
    [System.IO.File]::WriteAllText(
        (Join-Path $bin 'mvn.cmd'),
        "@echo off`r`necho executed> `"%PISTA_WRAPPER_MARKER%`"`r`nexit /b 0`r`n",
        [System.Text.Encoding]::ASCII)
    if ($Modified) {
        [System.IO.File]::WriteAllText((Join-Path $distribution 'modified.txt'), 'changed')
    }
    $archive = Join-Path $repo 'apache-maven-3.9.5-bin.zip'
    Compress-Archive -LiteralPath $distribution -DestinationPath $archive
    return $archive
}

function Invoke-TestWrapper([string]$Name, [string]$Archive, [string]$ExpectedHash) {
    $case = Join-Path $root $Name
    $project = Join-Path $case 'project'
    $temp = Join-Path $case 'temp'
    $cache = Join-Path $case 'cache'
    $marker = Join-Path $case 'executed.txt'
    $properties = Join-Path $project '.mvn\wrapper'
    New-Item -ItemType Directory -Path $properties, $temp, $cache -Force | Out-Null
    $wrapperCopy = Join-Path $project 'mvnw.cmd'
    Copy-Item -LiteralPath $Wrapper -Destination $wrapperCopy
    $uri = ([System.Uri]$Archive).AbsoluteUri
    [System.IO.File]::WriteAllLines(
        (Join-Path $properties 'maven-wrapper.properties'),
        @("distributionUrl=$uri", "distributionSha256Sum=$ExpectedHash"),
        [System.Text.Encoding]::ASCII)

    $start = New-Object System.Diagnostics.ProcessStartInfo
    $start.FileName = $env:ComSpec
    $start.Arguments = "/d /s /c `"`"$wrapperCopy`" -version`""
    $start.WorkingDirectory = $project
    $start.UseShellExecute = $false
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $start.EnvironmentVariables['MAVEN_USER_HOME'] = $cache
    $start.EnvironmentVariables['PISTA_WRAPPER_MARKER'] = $marker
    $start.EnvironmentVariables['TEMP'] = $temp
    $start.EnvironmentVariables['TMP'] = $temp
    foreach ($name in @('MVNW_REPOURL', 'MVNW_USERNAME', 'MVNW_PASSWORD')) {
        $start.EnvironmentVariables.Remove($name)
    }
    $process = [System.Diagnostics.Process]::Start($start)
    $stdout = $process.StandardOutput.ReadToEndAsync()
    $stderr = $process.StandardError.ReadToEndAsync()
    if (-not $process.WaitForExit(90000)) {
        $process.Kill()
        throw "Wrapper process timed out for $Name"
    }
    return @{
        ExitCode = $process.ExitCode
        Output = $stdout.GetAwaiter().GetResult() + $stderr.GetAwaiter().GetResult()
        Marker = $marker
        Cache = $cache
        Temp = $temp
    }
}

$good = New-TestArchive 'good' $false
$bad = New-TestArchive 'bad' $true
$expected = (Get-FileHash -LiteralPath $good -Algorithm SHA256).Hash.ToLowerInvariant()
$badHash = (Get-FileHash -LiteralPath $bad -Algorithm SHA256).Hash.ToLowerInvariant()
if ($expected -eq $badHash) { throw 'Test archives unexpectedly have the same checksum' }

$valid = Invoke-TestWrapper 'valid' $good $expected
if ($valid.ExitCode -ne 0 -or -not (Test-Path -LiteralPath $valid.Marker)) {
    throw "Valid archive did not execute successfully: $($valid.Output)"
}
if (-not (Get-ChildItem -Path $valid.Cache -Filter 'mvn.cmd' -Recurse -File)) {
    throw 'Valid archive was not installed in the wrapper cache'
}

$invalid = Invoke-TestWrapper 'invalid' $bad $expected
$downloaded = @(Get-ChildItem -Path $invalid.Temp -Filter 'apache-maven-3.9.5-bin.zip' -Recurse -File)
if ($downloaded.Count -ne 1 -or (Get-FileHash -LiteralPath $downloaded[0].FullName -Algorithm SHA256).Hash.ToLowerInvariant() -ne $badHash) {
    throw "Invalid case did not download the intended archive: $($invalid.Output)"
}
if ($invalid.ExitCode -eq 0 -or (Test-Path -LiteralPath $invalid.Marker)) {
    throw "Checksum mismatch did not stop execution: $($invalid.Output)"
}
if ($invalid.Output -notmatch 'Maven distribution SHA-256 validation failed') {
    throw "Checksum failure was not reported: $($invalid.Output)"
}
if ((Get-ChildItem -Path $invalid.Temp -Filter 'mvn.cmd' -Recurse -File) -or
    (Get-ChildItem -Path $invalid.Cache -Filter 'mvn.cmd' -Recurse -File)) {
    throw 'Invalid archive was extracted or installed'
}

Write-Output 'PASS: valid archive installed and executed'
Write-Output 'PASS: checksum mismatch rejected before extraction, caching, and execution'
Write-Output "Retained isolated test files: $root"
