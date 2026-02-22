$ErrorActionPreference = "Stop"

$serverDir = "C:\Users\the10\Projects\mc-server"
$repoDir = "C:\Users\the10\Projects\minecraft-god-mvp"
$javaExe = "C:\Program Files\Java\jdk-22\bin\java.exe"
if (-not (Test-Path $javaExe)) { $javaExe = "java" }

$logsDir = Join-Path $repoDir "playtest-logs"
New-Item -ItemType Directory -Force -Path $logsDir | Out-Null
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$bridgeOut = Join-Path $logsDir "bridge-$stamp.out.log"
$bridgeErr = Join-Path $logsDir "bridge-$stamp.err.log"
$testerOut = Join-Path $logsDir "tester-$stamp.log"
$latestLog = Join-Path $serverDir "logs\latest.log"

function Wait-LogPattern {
  param(
    [string]$Path,
    [string]$Pattern,
    [int]$TimeoutSec = 60
  )
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  while ((Get-Date) -lt $deadline) {
    if (Test-Path $Path) {
      $text = Get-Content $Path -Raw
      if ($text -match $Pattern) { return $true }
    }
    Start-Sleep -Milliseconds 300
  }
  return $false
}

function First-LineMatch {
  param(
    [string]$Path,
    [string]$Pattern
  )
  if (-not (Test-Path $Path)) { return "" }
  return (Get-Content $Path | Where-Object { $_ -match $Pattern } | Select-Object -First 1)
}

$server = $null
$bridge = $null
$spinupError = ""

$versionLine = ""
$bindLine = ""
$listLine = ""
$bridgeStartOk = $false
$spawnMara = $false
$spawnEli = $false
$spawnNox = $false
$testerResultsLine = ""
$testerRepliesLine = ""
$testerErrors = @()

try {
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $javaExe
  $psi.Arguments = "-Xms1G -Xmx2G -jar .\paper.jar nogui"
  $psi.WorkingDirectory = $serverDir
  $psi.UseShellExecute = $false
  $psi.RedirectStandardInput = $true
  $psi.RedirectStandardOutput = $false
  $psi.RedirectStandardError = $false
  $psi.CreateNoWindow = $true
  $server = New-Object System.Diagnostics.Process
  $server.StartInfo = $psi
  [void]$server.Start()

  if (-not (Wait-LogPattern -Path $latestLog -Pattern "Done \(" -TimeoutSec 160)) {
    throw "Server did not reach ready state."
  }

  $versionLine = First-LineMatch -Path $latestLog -Pattern "Loading Paper 1\.21\.11.*Minecraft 1\.21\.11"
  $bindLine = First-LineMatch -Path $latestLog -Pattern "Starting Minecraft server on 127\.0\.0\.1:25565"

  $server.StandardInput.WriteLine("difficulty peaceful")
  $server.StandardInput.WriteLine("gamerule domobspawning false")
  $server.StandardInput.WriteLine("gamerule keepinventory true")
  $server.StandardInput.WriteLine("time set day")
  $server.StandardInput.WriteLine("gamerule dodaylightcycle false")
  Start-Sleep -Seconds 2

  $bridge = Start-Process -FilePath "C:\Program Files\nodejs\node.exe" -ArgumentList ".\src\minecraftBridge.js" -WorkingDirectory $repoDir -RedirectStandardOutput $bridgeOut -RedirectStandardError $bridgeErr -PassThru

  $bridgeStartOk = Wait-LogPattern -Path $bridgeOut -Pattern "\[Bridge\] starting Mineflayer bots\.\.\. host=127\.0\.0\.1 port=25565 version=1\.21\.11" -TimeoutSec 60
  $spawnMara = Wait-LogPattern -Path $bridgeOut -Pattern "\[Bridge\] mara spawned on 127\.0\.0\.1:25565" -TimeoutSec 90
  $spawnEli = Wait-LogPattern -Path $bridgeOut -Pattern "\[Bridge\] eli spawned on 127\.0\.0\.1:25565" -TimeoutSec 90
  $spawnNox = Wait-LogPattern -Path $bridgeOut -Pattern "\[Bridge\] nox spawned on 127\.0\.0\.1:25565" -TimeoutSec 90

  $server.StandardInput.WriteLine("list")
  Start-Sleep -Seconds 2
  $listLine = First-LineMatch -Path $latestLog -Pattern "players online"

  $testerScript = @'
const mineflayer = require("mineflayer");
const bot = mineflayer.createBot({
  host: "127.0.0.1",
  port: 25565,
  version: "1.21.11",
  username: `playtest_${Date.now() % 100000}`,
});
const prefix = "!";
const results = { mara: false, eli: false, nox: false };
const replies = [];
let spawned = false;

bot.once("spawn", () => {
  spawned = true;
  setTimeout(() => bot.chat(`${prefix}mara hello`), 1000);
  setTimeout(() => bot.chat(`${prefix}eli hello`), 2500);
  setTimeout(() => bot.chat(`${prefix}nox hello`), 4000);
});

bot.on("chat", (username, message) => {
  const key = String(username || "").toLowerCase();
  if (Object.prototype.hasOwnProperty.call(results, key)) {
    results[key] = true;
    replies.push(`${key}: ${message}`);
  }
});

bot.on("kicked", (reason) => console.log(`TESTER_KICKED=${reason}`));
bot.on("error", (err) => console.log(`TESTER_ERROR=${err && err.message ? err.message : String(err)}`));

setTimeout(() => {
  if (!spawned) console.log("TESTER_ERROR=spawn_timeout");
  console.log(`PLAYTEST_RESULTS=${JSON.stringify(results)}`);
  console.log(`PLAYTEST_REPLIES=${JSON.stringify(replies)}`);
  try { bot.quit("done"); } catch {}
  process.exit(0);
}, 22000);
'@

  $testerOutput = $testerScript | & "C:\Program Files\nodejs\node.exe" -
  $testerOutput | Set-Content $testerOut
  $testerResultsLine = ($testerOutput | Where-Object { $_ -like "PLAYTEST_RESULTS=*" } | Select-Object -First 1)
  $testerRepliesLine = ($testerOutput | Where-Object { $_ -like "PLAYTEST_REPLIES=*" } | Select-Object -First 1)
  $testerErrors = @($testerOutput | Where-Object { $_ -like "TESTER_*" })
}
catch {
  $spinupError = $_.Exception.Message
}
finally {
  if ($bridge -and -not $bridge.HasExited) {
    try { Stop-Process -Id $bridge.Id -Force } catch {}
  }
  if ($server -and -not $server.HasExited) {
    try { $server.StandardInput.WriteLine("stop") } catch {}
    if (-not $server.WaitForExit(60000)) {
      try { Stop-Process -Id $server.Id -Force } catch {}
    }
  }
}

if ($spinupError) {
  Write-Output "SPINUP_ERROR=$spinupError"
}
Write-Output "SERVER_LOG=$latestLog"
Write-Output "BRIDGE_STDOUT_LOG=$bridgeOut"
Write-Output "BRIDGE_STDERR_LOG=$bridgeErr"
Write-Output "TESTER_LOG=$testerOut"
Write-Output "SERVER_VERSION_LINE=$versionLine"
Write-Output "SERVER_BIND_LINE=$bindLine"
Write-Output "SERVER_LIST_LINE=$listLine"
Write-Output "BRIDGE_START_OK=$bridgeStartOk"
Write-Output "BOT_SPAWN_MARA=$spawnMara"
Write-Output "BOT_SPAWN_ELI=$spawnEli"
Write-Output "BOT_SPAWN_NOX=$spawnNox"
Write-Output "TESTER_RESULTS_LINE=$testerResultsLine"
Write-Output "TESTER_REPLIES_LINE=$testerRepliesLine"
if ($testerErrors.Count -gt 0) {
  Write-Output "TESTER_ERRORS_BEGIN"
  $testerErrors | ForEach-Object { Write-Output $_ }
  Write-Output "TESTER_ERRORS_END"
}
if (Test-Path $bridgeErr) {
  $bridgeErrLines = Get-Content $bridgeErr
  if ($bridgeErrLines.Count -gt 0) {
    Write-Output "BRIDGE_ERRORS_BEGIN"
    $bridgeErrLines | Select-Object -First 40 | ForEach-Object { Write-Output $_ }
    Write-Output "BRIDGE_ERRORS_END"
  }
}
