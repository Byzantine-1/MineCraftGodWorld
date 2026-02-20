const { spawn } = require('child_process')

function runCliSmoke() {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['./src/index.js'], {
      cwd: process.cwd(),
      stdio: ['pipe', 'pipe', 'pipe']
    })

    let stderr = ''
    let stdout = ''
    let started = false

    child.stdout.on('data', (d) => {
      stdout += String(d)
      if (!started && stdout.includes('--- WORLD ONLINE ---')) {
        started = true
        child.kill('SIGTERM')
      }
    })

    child.stderr.on('data', (d) => {
      stderr += String(d)
    })

    const timeout = setTimeout(() => {
      child.kill('SIGTERM')
      reject(new Error('Smoke test timed out waiting for CLI startup'))
    }, 15000)

    child.on('close', (code) => {
      clearTimeout(timeout)
      if (!started) {
        reject(new Error(`Smoke test failed with exit code ${code}\n${stderr}`))
        return
      }
      resolve()
    })
  })
}

runCliSmoke()
  .then(() => {
    console.log('Smoke passed: CLI started successfully.')
  })
  .catch((err) => {
    console.error(err.message || err)
    process.exitCode = 1
  })
