// src/minecraftBridge.js
require("dotenv").config();

const mineflayer = require("mineflayer");
const { spawn } = require("child_process");

const HOST = process.env.MC_HOST || "127.0.0.1";
const PORT = parseInt(process.env.MC_PORT || "25565", 10);
const VERSION = process.env.MC_VERSION || ""; // empty => auto
const BOT_NAMES = (process.env.BOT_NAMES || "mara,eli,nox")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const CHAT_PREFIX = process.env.CHAT_PREFIX || "";

// Spawn upgraded CLI world engine (do not modify engine files)
const engine = spawn(process.execPath, ["./src/index.js"], {
  cwd: process.cwd(),
  stdio: ["pipe", "pipe", "pipe"],
});

engine.on("exit", (code) => {
  console.error(`[Bridge] world engine exited with code ${code}`);
});

engine.stderr.on("data", (d) => {
  process.stderr.write(`[Engine STDERR] ${d.toString()}`);
});

// Expected engine output: "Mara: ..." "Eli: ..." "Nox: ..."
const replyRegex = /^\s*>?\s*(Mara|Eli|Nox)\s*:\s*(.+)\s*$/i;

const bots = new Map();     // lowerName -> bot
const lastSaid = new Map(); // lowerName -> lastMessage

function sendToEngine(line) {
  engine.stdin.write(line.trimEnd() + "\n");
}

function safeChat(bot, msg) {
  const key = bot.username.toLowerCase();
  if (lastSaid.get(key) === msg) return;
  lastSaid.set(key, msg);
  bot.chat(msg);
}

engine.stdout.on("data", (d) => {
  const text = d.toString();
  for (const rawLine of text.split(/\r?\n/)) {
    if (!rawLine.trim()) continue;
    const m = rawLine.match(replyRegex);
    if (!m) continue;

    const agentName = m[1].toLowerCase();
    const message = m[2].trim();

    const bot = bots.get(agentName);
    if (!bot) continue;

    safeChat(bot, message);
  }
});

function parseIncomingChat(message) {
  const trimmed = message.trim();
  if (CHAT_PREFIX && !trimmed.startsWith(CHAT_PREFIX)) return null;

  const withoutPrefix = CHAT_PREFIX ? trimmed.slice(CHAT_PREFIX.length).trim() : trimmed;
  const parts = withoutPrefix.split(" ");
  if (parts.length < 2) return null;

  const target = parts[0].toLowerCase();
  const content = parts.slice(1).join(" ").trim();
  if (!bots.has(target)) return null;
  if (!content) return null;

  return { target, content };
}

function startBot(name) {
  const bot = mineflayer.createBot({
    host: HOST,
    port: PORT,
    username: name,
    ...(VERSION ? { version: VERSION } : {}),
  });

  bot.once("spawn", () => {
    console.log(`[Bridge] ${name} spawned on ${HOST}:${PORT}`);
  });

  bot.on("chat", (username, message) => {
    if (!username) return;
    const u = username.toLowerCase();
    const self = bot.username.toLowerCase();

    // Ignore self and other bots to prevent loops
    if (u === self) return;
    if (BOT_NAMES.map((n) => n.toLowerCase()).includes(u)) return;

    const parsed = parseIncomingChat(message);
    if (!parsed) return;

    // Use existing engine command interface
    sendToEngine(`talk ${parsed.target} ${parsed.content}`);
  });

  bot.on("kicked", (reason) => console.warn(`[Bridge] ${name} kicked:`, reason));
  bot.on("error", (err) => console.error(`[Bridge] ${name} error:`, err));

  return bot;
}

console.log(`[Bridge] starting Mineflayer bots... host=${HOST} port=${PORT} version=${VERSION || "(auto)"}`);
console.log(`[Bridge] chat usage: "${CHAT_PREFIX}mara hello" / "${CHAT_PREFIX}eli hello" / "${CHAT_PREFIX}nox hello"`);

for (const n of BOT_NAMES) {
  bots.set(n.toLowerCase(), startBot(n));
}

// Clean shutdown
process.on("SIGINT", () => {
  console.log("\n[Bridge] shutting down...");
  try { sendToEngine("exit"); } catch {}
  for (const bot of bots.values()) {
    try { bot.quit("bridge shutdown"); } catch {}
  }
  process.exit(0);
});
