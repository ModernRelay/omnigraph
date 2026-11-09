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
$admissionInstalled = $false

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

  # "ok"; "missing" when the release or a file is not published (HTTP 404);
  # "failed" on any other error.
  try {
    Invoke-WebRequest -UseBasicParsing -Uri "$BaseUrl/$assetName" -OutFile $ArchivePath
    Invoke-WebRequest -UseBasicParsing -Uri "$BaseUrl/$assetStem.sha256" -OutFile $ChecksumPath
    return "ok"
  } catch {
    $response = $_.Exception.Response
    if ($null -ne $response -and [int]$response.StatusCode -eq 404) {
      return "missing"
    }
    return "failed"
  }
}

function Verify-Attestation {
  param([string]$ArchivePath)

  $mode = if ($env:VERIFY_ATTESTATION) { $env:VERIFY_ATTESTATION } else { "auto" }
  if ($mode -eq "off") {
    Write-Log "Skipping build provenance verification (VERIFY_ATTESTATION=off)"
    return
  }
  if ($mode -ne "auto" -and $mode -ne "require") {
    throw "unsupported VERIFY_ATTESTATION '$mode' (expected auto, require or off)"
  }

  $signedIn = $false
  if (Get-Command gh -ErrorAction SilentlyContinue) {
    # gh before 2.49 has no `attestation` command; treat it as absent.
    & gh attestation verify --help *> $null
    if ($LASTEXITCODE -eq 0) {
      & gh auth status *> $null
      $signedIn = ($LASTEXITCODE -eq 0)
    }
  }
  if (!$signedIn) {
    if ($mode -eq "require") {
      throw "VERIFY_ATTESTATION=require needs the GitHub CLI (gh), signed in"
    }
    Write-Log "Build provenance not verified: install and sign in to the GitHub CLI (gh) to check it"
    return
  }

  & gh attestation verify $ArchivePath --repo $RepoSlug *> $null
  if ($LASTEXITCODE -eq 0) {
    Write-Log "Verified build provenance of $assetName with gh attestation verify"
    return
  }
  if ($mode -eq "auto" -and $Version -ne "") {
    # An explicitly named release may predate attestations (before v0.14).
    Write-Log "warning: no valid build attestation for $Version; it may predate attestations"
    return
  }
  throw "build provenance verification failed for $assetName; refusing to install. Set VERIFY_ATTESTATION=off only if you trust this archive"
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
  $sourceAdmission = Join-Path $SourceDir "omnigraph-azure-admission.exe"
  $destinationAdmission = Join-Path $InstallDir "omnigraph-azure-admission.exe"
  if (Test-Path $sourceAdmission) {
    Copy-Item -Path $sourceAdmission -Destination $destinationAdmission -Force
    $script:admissionInstalled = $true
  } else {
    # Releases before native Azure support contain only the CLI and server.
    Remove-Item -Path $destinationAdmission -Force -ErrorAction SilentlyContinue
    $script:admissionInstalled = $false
  }
}

function Install-FromRelease {
  New-Item -ItemType Directory -Force -Path $workDir | Out-Null

  $archivePath = Join-Path $workDir $assetName
  $checksumPath = Join-Path $workDir "$assetStem.sha256"

  if ($Version -ne "") {
    $script:selectedChannel = $Version
    $baseUrl = Get-ReleaseBaseUrl -Channel $ReleaseChannel
    Write-Log "Downloading $assetName from $Version"
    if ((Download-ReleaseFiles -BaseUrl $baseUrl -ArchivePath $archivePath -ChecksumPath $checksumPath) -ne "ok") {
      throw "no published binary found for $assetName at release $Version"
    }
  } else {
    $script:selectedChannel = $ReleaseChannel
    $baseUrl = Get-ReleaseBaseUrl -Channel $selectedChannel
    Write-Log "Downloading $assetName from $selectedChannel"
    $download = Download-ReleaseFiles -BaseUrl $baseUrl -ArchivePath $archivePath -ChecksumPath $checksumPath
    if ($download -ne "ok") {
      if ($ReleaseChannel -ne "stable") {
        throw "no published binary found for $assetName on channel $ReleaseChannel"
      }
      if ($download -ne "missing") {
        # Only a stable release that is not published falls back; a network
        # or server failure must not switch the channel.
        throw "download of $assetName from the stable channel failed; retry, or pass -ReleaseChannel edge"
      }

      Write-Log "No stable release publishes $assetName (HTTP 404); installing the edge build instead"
      $script:selectedChannel = "edge"
      $baseUrl = Get-ReleaseBaseUrl -Channel $selectedChannel
      if ((Download-ReleaseFiles -BaseUrl $baseUrl -ArchivePath $archivePath -ChecksumPath $checksumPath) -ne "ok") {
        throw "no published binary found for $assetName on stable or edge; build from source"
      }
    }
  }

  Verify-Checksum -ArchivePath $archivePath -ChecksumPath $checksumPath
  Verify-Attestation -ArchivePath $archivePath

  $extractDir = Join-Path $workDir "extract"
  New-Item -ItemType Directory -Force -Path $extractDir | Out-Null
  Expand-Archive -Path $archivePath -DestinationPath $extractDir -Force
  Install-FromDirectory -SourceDir $extractDir
}

function Print-Summary {
  $omnigraphPath = Join-Path $InstallDir "omnigraph.exe"
  $serverPath = Join-Path $InstallDir "omnigraph-server.exe"
  $admissionPath = Join-Path $InstallDir "omnigraph-azure-admission.exe"

  Write-Host ""
  Write-Host "Installed:"
  Write-Host "  $omnigraphPath"
  Write-Host "  $serverPath"
  Write-Host ""
  Write-Host "Verify:"
  Write-Host "  $omnigraphPath version"
  Write-Host "  $serverPath --help"
  if ($admissionInstalled) {
    Write-Host "  $admissionPath"
    Write-Host ""
    Write-Host "Verify Azure admission:"
    Write-Host "  $admissionPath --help"
  }
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
