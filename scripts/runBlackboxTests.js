const fs = require('fs')
const path = require('path')
const { spawn } = require('child_process')

/**
 * @param {string} command
 * @param {string[]} args
 * @param {string} cwd
 * @returns {Promise<number>}
 */
function runCommand(command, args, cwd) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      stdio: 'inherit',
      shell: true
    })
    child.on('error', reject)
    child.on('close', (code) => {
      resolve(Number(code || 0))
    })
  })
}

async function main() {
  const configuredDir = String(process.env.BLACKBOX_TEST_DIR || '').trim()
  if (!configuredDir) {
    console.log('BLACKBOX_TESTS: SKIPPED (BLACKBOX_TEST_DIR not set)')
    return
  }

  const blackboxDir = path.resolve(configuredDir)
  const packageJsonPath = path.join(blackboxDir, 'package.json')

  let stat
  try {
    stat = await fs.promises.stat(blackboxDir)
  } catch (err) {
    console.error(`BLACKBOX_TESTS: FAILED (directory not found: ${blackboxDir})`)
    process.exitCode = 1
    return
  }
  if (!stat.isDirectory()) {
    console.error(`BLACKBOX_TESTS: FAILED (path is not a directory: ${blackboxDir})`)
    process.exitCode = 1
    return
  }

  try {
    await fs.promises.access(packageJsonPath, fs.constants.F_OK)
  } catch (err) {
    console.error(`BLACKBOX_TESTS: FAILED (missing package.json in ${blackboxDir})`)
    process.exitCode = 1
    return
  }

  console.log(`BLACKBOX_TESTS: RUNNING from ${blackboxDir}`)

  let installCode = await runCommand('npm', ['ci'], blackboxDir)
  if (installCode !== 0) {
    console.warn('BLACKBOX_TESTS: npm ci failed, falling back to npm install')
    installCode = await runCommand('npm', ['install'], blackboxDir)
    if (installCode !== 0) {
      process.exitCode = installCode
      return
    }
  }

  const testCode = await runCommand('npm', ['test'], blackboxDir)
  process.exitCode = testCode
}

main().catch((err) => {
  console.error(err instanceof Error ? err.stack || err.message : String(err))
  process.exitCode = 1
})
