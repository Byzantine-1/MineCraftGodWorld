const fs = require('fs')
const path = require('path')

const SOAK_OUT_PATH = path.resolve(__dirname, '../docs/playtest-macro.txt')
const SMOKE_OUT_PATH = path.resolve(__dirname, '../docs/playtest-smoke-macro.txt')
const DEFAULT_NAMES = ['mara', 'eli']

/**
 * @param {number} n
 */
function pad3(n) {
  return String(n).padStart(3, '0')
}

/**
 * @returns {string[]}
 */
function resolveNames() {
  const parsed = String(process.env.PLAYTEST_MACRO_NAMES || '')
    .split(',')
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean)
  return parsed.length > 0 ? parsed : DEFAULT_NAMES
}

/**
 * @param {number} i
 * @param {string[]} names
 */
function makeChatLine(i, names) {
  const name = names[(i - 1) % names.length]
  const id = pad3(i)
  const bucket = i % 14

  if (bucket === 0) return `${name} ping ${id}...`
  if (bucket === 1) return `${name} ping ${id}`
  if (bucket === 2) return `${name} ping ${id}!`
  if (bucket === 3) return `${name} ping ${id}?`
  if (bucket === 4) return `${name}   ping   ${id}`
  if (bucket === 5) return `${name} route-check alpha_hall ${id}`
  if (bucket === 6) return `${name} punctuation !!! ??? ### ${id}`
  if (bucket === 7) return `${name} punctuation ;;; ::: ,,, ${id}`
  if (bucket === 8) return `${name} punctuation (( )) [[ ]] ${id}`
  if (bucket === 9) return `${name} long ${'x'.repeat(24)} ${id}`
  if (bucket === 10) return `${name} mixed alpha-beta_gamma.${id}`
  if (bucket === 11) return `${name} spacing   and-tabs-like   ${id}`
  if (bucket === 12) return `${name} route risk-check dusk ${id}`
  return `${name} final-check ${id}`
}

/**
 * @param {number} count
 * @param {string[]} names
 * @returns {string[]}
 */
function buildChatLines(count, names) {
  const lines = []
  for (let i = 1; i <= count; i += 1) {
    lines.push(makeChatLine(i, names))
  }
  return lines
}

/**
 * @returns {string[]}
 */
function buildCliBlock() {
  return [
    '# Optional CLI follow-up (run in node src/index.js, not Minecraft chat):',
    '# Replace <town> and <contract_id> first.',
    'god contract list <town>',
    'god contract show <contract_id>',
    'god contract accept Mara <contract_id>',
    'god quest show <contract_id>',
    'god balance Mara'
  ]
}

/**
 * @param {string} title
 * @param {string[]} chatLines
 * @returns {string}
 */
function renderMacro(title, chatLines) {
  const sections = [
    `# ${title}`,
    '# Minecraft chat section (paste in batches of 10-20 lines):',
    ...chatLines,
    '',
    ...buildCliBlock(),
    ''
  ]
  return sections.join('\n')
}

const names = resolveNames()
const smokeLines = buildChatLines(12, names)
const soakLines = buildChatLines(84, names)

fs.writeFileSync(SMOKE_OUT_PATH, renderMacro('In-Game Smoke Macro (12 lines)', smokeLines), 'utf8')
fs.writeFileSync(SOAK_OUT_PATH, renderMacro('In-Game Soak Macro (84 lines)', soakLines), 'utf8')

process.stdout.write(`Wrote ${smokeLines.length} lines to ${SMOKE_OUT_PATH}\n`)
process.stdout.write(`Wrote ${soakLines.length} lines to ${SOAK_OUT_PATH}\n`)
