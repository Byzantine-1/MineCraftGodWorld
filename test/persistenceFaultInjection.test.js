const fs = require('fs')
const os = require('os')
const path = require('path')
const crypto = require('crypto')
const test = require('node:test')
const assert = require('node:assert/strict')

const { createMemoryStore } = require('../src/memory')

function createTempMemoryPath() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mvp-memory-faults-'))
  return path.join(dir, 'memory.json')
}

function fileHash(filePath) {
  const payload = fs.readFileSync(filePath)
  return crypto.createHash('sha256').update(payload).digest('hex')
}

function listTempArtifacts(filePath) {
  const dir = path.dirname(filePath)
  const base = path.basename(filePath)
  return fs.readdirSync(dir).filter((name) => name.startsWith(`${base}.`) && name.endsWith('.tmp'))
}

function createFaultFs({ failRenameOnce = false, failWriteOnce = false } = {}) {
  let renameFailed = false
  let writeFailed = false
  return {
    ...fs,
    promises: {
      ...fs.promises,
      open: (...args) => fs.promises.open(...args),
      readFile: (...args) => fs.promises.readFile(...args),
      writeFile: async (...args) => {
        if (failWriteOnce && !writeFailed) {
          writeFailed = true
          const err = new Error('disk full')
          err.code = 'ENOSPC'
          throw err
        }
        return fs.promises.writeFile(...args)
      },
      rename: async (...args) => {
        if (failRenameOnce && !renameFailed) {
          renameFailed = true
          const err = new Error('rename denied')
          err.code = 'EACCES'
          throw err
        }
        return fs.promises.rename(...args)
      },
      unlink: (...args) => fs.promises.unlink(...args)
    }
  }
}

test('memory store recovers after rename failure with no partial snapshot', async () => {
  const filePath = createTempMemoryPath()
  const seedStore = createMemoryStore({ filePath })
  await seedStore.transact((memory) => {
    memory.world.player.legitimacy = 73
  }, { eventId: 'seed-legit' })

  const beforeHash = fileHash(filePath)
  const faultStore = createMemoryStore({
    filePath,
    fsModule: createFaultFs({ failRenameOnce: true })
  })

  await assert.rejects(
    faultStore.transact((memory) => {
      memory.world.player.legitimacy = 5
    }, { eventId: 'rename-failure-op' }),
    (err) => err && err.code === 'MEMORY_WRITE_FAILED'
  )

  assert.equal(fileHash(filePath), beforeHash, 'failed rename must not corrupt previous committed snapshot')
  assert.deepEqual(listTempArtifacts(filePath), [], 'temp artifacts should be cleaned after failed persist')
  assert.equal(fs.existsSync(`${filePath}.lock`), false, 'lock file must be released after failed persist')

  const recoveryStore = createMemoryStore({ filePath })
  const tx = await recoveryStore.transact((memory) => {
    memory.world.player.legitimacy = 11
  }, { eventId: 'rename-recovery-op' })
  assert.equal(tx.skipped, false)
  assert.equal(recoveryStore.getSnapshot().world.player.legitimacy, 11)
})

test('memory store recovers after write failure with no partial snapshot', async () => {
  const filePath = createTempMemoryPath()
  const seedStore = createMemoryStore({ filePath })
  await seedStore.transact((memory) => {
    memory.world.player.legitimacy = 61
  }, { eventId: 'seed-legit-write' })

  const beforeHash = fileHash(filePath)
  const faultStore = createMemoryStore({
    filePath,
    fsModule: createFaultFs({ failWriteOnce: true })
  })

  await assert.rejects(
    faultStore.transact((memory) => {
      memory.world.player.legitimacy = 17
    }, { eventId: 'write-failure-op' }),
    (err) => err && err.code === 'MEMORY_WRITE_FAILED'
  )

  assert.equal(fileHash(filePath), beforeHash, 'failed write must not corrupt previous committed snapshot')
  assert.deepEqual(listTempArtifacts(filePath), [], 'temp artifacts should be cleaned after failed write')
  assert.equal(fs.existsSync(`${filePath}.lock`), false, 'lock file must be released after failed write')

  const recoveryStore = createMemoryStore({ filePath })
  const tx = await recoveryStore.transact((memory) => {
    memory.world.player.legitimacy = 22
  }, { eventId: 'write-recovery-op' })
  assert.equal(tx.skipped, false)
  assert.equal(recoveryStore.getSnapshot().world.player.legitimacy, 22)
})

test('simulated crash during transaction releases lock and preserves durability', async () => {
  const filePath = createTempMemoryPath()
  const arg = '--simulate-crash'
  const hadArg = process.argv.includes(arg)
  const originalRandom = Math.random
  if (!hadArg) process.argv.push(arg)
  Math.random = () => 0.01

  try {
    const crashStore = createMemoryStore({ filePath })
    await assert.rejects(
      crashStore.transact((memory) => {
        memory.world.player.legitimacy = 3
      }, { eventId: 'sim-crash-op' }),
      (err) => err && err.code === 'SIMULATED_CRASH'
    )

    assert.equal(fs.existsSync(`${filePath}.lock`), false, 'lock file must be removed even on simulated crash')
    const text = fs.existsSync(filePath) ? fs.readFileSync(filePath, 'utf8') : '{}'
    JSON.parse(text)
  } finally {
    Math.random = originalRandom
    if (!hadArg) {
      const idx = process.argv.indexOf(arg)
      if (idx >= 0) process.argv.splice(idx, 1)
    }
  }

  const recoveryStore = createMemoryStore({ filePath })
  const tx = await recoveryStore.transact((memory) => {
    memory.world.player.legitimacy = 29
  }, { eventId: 'after-sim-crash' })
  assert.equal(tx.skipped, false)
  assert.equal(recoveryStore.getSnapshot().world.player.legitimacy, 29)
  assert.equal(recoveryStore.validateMemoryIntegrity().ok, true)
})
