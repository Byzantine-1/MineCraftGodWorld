param(
  [string]$Host = "127.0.0.1",
  [int]$Port = 25565,
  [string]$Version = "",
  [string]$BotNames = "mara,eli,nox",
  [string]$ChatPrefix = ""
)

$ErrorActionPreference = "Stop"

$env:MC_HOST = $Host
$env:MC_PORT = [string]$Port

if ([string]::IsNullOrWhiteSpace($Version)) {
  Remove-Item Env:MC_VERSION -ErrorAction SilentlyContinue
} else {
  $env:MC_VERSION = $Version
}

$env:BOT_NAMES = $BotNames
$env:CHAT_PREFIX = $ChatPrefix

Write-Host "[Playtest] MC_HOST=$($env:MC_HOST) MC_PORT=$($env:MC_PORT) MC_VERSION=$($env:MC_VERSION) BOT_NAMES=$($env:BOT_NAMES) CHAT_PREFIX=$($env:CHAT_PREFIX)"
npm run bots
