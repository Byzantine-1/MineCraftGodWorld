const fs = require("fs");
const path = require("path");

const OUT_PATH = path.resolve(__dirname, "../docs/playtest-macro.txt");
const NAMES = ["mara", "eli", "nox"];

/**
 * @param {number} n
 */
function pad3(n) {
  return String(n).padStart(3, "0");
}

/**
 * @param {number} i
 */
function makeLine(i) {
  const name = NAMES[(i - 1) % NAMES.length];
  const id = pad3(i);
  const bucket = i % 12;

  if (bucket === 0) return `${name} ping ${id}...`;
  if (bucket === 1) return `${name} ping ${id}`;
  if (bucket === 2) return `${name} ping ${id}!`;
  if (bucket === 3) return `${name} ping ${id}?`;
  if (bucket === 4) return `${name}   ping   ${id}`;
  if (bucket === 5) return `${name} route-check alpha_hall ${id}`;
  if (bucket === 6) return `${name} punctuation !!! ??? ### ${id}`;
  if (bucket === 7) return `${name} punctuation ;;; ::: ,,, ${id}`;
  if (bucket === 8) return `${name} punctuation (( )) [[ ]] ${id}`;
  if (bucket === 9) return `${name} long ${"x".repeat(24)} ${id}`;
  if (bucket === 10) return `${name} mixed alpha-beta_gamma.${id}`;
  return `${name} final-check ${id}`;
}

const lines = [];
for (let i = 1; i <= 72; i += 1) {
  lines.push(makeLine(i));
}

const content = `${lines.join("\n")}\n`;
fs.writeFileSync(OUT_PATH, content, "utf8");
process.stdout.write(`Wrote ${lines.length} lines to ${OUT_PATH}\n`);
