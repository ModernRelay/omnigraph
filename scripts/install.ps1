param(
  [string]$RepoSlug = "ModernRelay/omnigraph",
  [string]$InstallDir = "$env:USERPROFILE\.local\bin",
  [ValidateSet("stable", "edge")]
  [string]$ReleaseChannel = "stable",
  [string]$Version = ""
)

$ErrorActionPreference = "Stop"

$assetName = "omnigraph-windows-x86_64.zip"
$assetStem = "omnigraph-windows-x86_64"
$workDir = Join-Path ([System.IO.Path]::GetTempPath()) ("omnigraph-install-" + [System.Guid]::NewGuid().ToString("N"))
$selectedChannel = ""

function Write-Log {
  param([string]$Message)
  Write-Host "==> $Message"
}

function Get-ReleaseBaseUrl {
  param([string]$Channel)

  if ($Version -ne "") {
    return "https://github.com/$RepoSlug/releases/download/$Version"
  }

  if ($Channel -eq "stable") {
    return "https://github.com/$RepoSlug/releases/latest/download"
  }

  if ($Channel -eq "edge") {
    return "https://github.com/$RepoSlug/releases/download/edge"
  }

  throw "unsupported ReleaseChannel '$Channel' (expected stable or edge)"
}

function Download-ReleaseFiles {
  param(
    [string]$BaseUrl,
    [string]$ArchivePath,
    [string]$ChecksumPath
  )

  try {
    Invoke-WebRequest -UseBasicParsing -Uri "$BaseUrl/$assetName" -OutFile $ArchivePath
    Invoke-WebRequest -UseBasicParsing -Uri "$BaseUrl/$assetStem.sha256" -OutFile $ChecksumPath
    return $true
  } catch {
    return $false
  }
}

function Verify-Checksum {
  param(
    [string]$ArchivePath,
    [string]$ChecksumPath
  )

  $checksumText = (Get-Content -Path $ChecksumPath -Raw).Trim()
  $expected = ($checksumText -split "\s+")[0].ToLowerInvariant()
  if ($expected -eq "") {
    throw "checksum file did not contain a SHA256 digest"
  }

  $actual = (Get-FileHash -Path $ArchivePath -Algorithm SHA256).Hash.ToLowerInvariant()
  if ($actual -ne $expected) {
    throw "checksum verification failed for $assetName"
  }
}

function Install-FromDirectory {
  param([string]$SourceDir)

  New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
  Copy-Item -Path (Join-Path $SourceDir "omnigraph.exe") -Destination (Join-Path $InstallDir "omnigraph.exe") -Force
  Copy-Item -Path (Join-Path $SourceDir "omnigraph-server.exe") -Destination (Join-Path $InstallDir "omnigraph-server.exe") -Force
}

function Install-FromRelease {
  New-Item -ItemType Directory -Force -Path $workDir | Out-Null

  $archivePath = Join-Path $workDir $assetName
  $checksumPath = Join-Path $workDir "$assetStem.sha256"

  if ($Version -ne "") {
    $script:selectedChannel = $Version
    $baseUrl = Get-ReleaseBaseUrl -Channel $ReleaseChannel
    Write-Log "Downloading $assetName from $Version"
    if (!(Download-ReleaseFiles -BaseUrl $baseUrl -ArchivePath $archivePath -ChecksumPath $checksumPath)) {
      throw "no published binary found for $assetName at release $Version"
    }
  } else {
    $script:selectedChannel = $ReleaseChannel
    $baseUrl = Get-ReleaseBaseUrl -Channel $selectedChannel
    Write-Log "Downloading $assetName from $selectedChannel"
    if (!(Download-ReleaseFiles -BaseUrl $baseUrl -ArchivePath $archivePath -ChecksumPath $checksumPath)) {
      if ($ReleaseChannel -ne "stable") {
        throw "no published binary found for $assetName on channel $ReleaseChannel"
      }

      Write-Log "Stable release binaries are not published yet; falling back to edge"
      $script:selectedChannel = "edge"
      $baseUrl = Get-ReleaseBaseUrl -Channel $selectedChannel
      if (!(Download-ReleaseFiles -BaseUrl $baseUrl -ArchivePath $archivePath -ChecksumPath $checksumPath)) {
        throw "no published binary found for $assetName on stable or edge; build from source"
      }
    }
  }

  Verify-Checksum -ArchivePath $archivePath -ChecksumPath $checksumPath

  $extractDir = Join-Path $workDir "extract"
  New-Item -ItemType Directory -Force -Path $extractDir | Out-Null
  Expand-Archive -Path $archivePath -DestinationPath $extractDir -Force
  Install-FromDirectory -SourceDir $extractDir
}

function Print-Summary {
  $omnigraphPath = Join-Path $InstallDir "omnigraph.exe"
  $serverPath = Join-Path $InstallDir "omnigraph-server.exe"

  Write-Host ""
  Write-Host "Installed:"
  Write-Host "  $omnigraphPath"
  Write-Host "  $serverPath"
  Write-Host ""
  Write-Host "Verify:"
  Write-Host "  $omnigraphPath version"
  Write-Host "  $serverPath --help"
  Write-Host ""

  if ($selectedChannel -ne "") {
    Write-Host "Installed from release channel: $selectedChannel"
  }

  $pathParts = $env:Path -split [System.IO.Path]::PathSeparator
  if ($pathParts -notcontains $InstallDir) {
    Write-Host "Add $InstallDir to PATH if needed."
  }
}

try {
  Install-FromRelease
  Print-Summary
} finally {
  if (Test-Path $workDir) {
    Remove-Item -Path $workDir -Recurse -Force
  }
}
