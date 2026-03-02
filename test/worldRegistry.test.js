const fs = require('fs')
const os = require('os')
const path = require('path')
const test = require('node:test')
const assert = require('node:assert/strict')

const { createMemoryStore } = require('../src/memory')
const { createWorldRegistryStore } = require('../src/worldRegistry')

function createTempMemoryPath() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mvp-world-registry-'))
  return path.join(dir, 'memory.json')
}

test('world registry exposes multiple towns and stable officeholder lookup', () => {
  const filePath = createTempMemoryPath()
  fs.writeFileSync(filePath, JSON.stringify({
    world: {
      towns: {
        alpha: {
          name: 'Ironvale',
          status: 'active',
          region: 'lowlands',
          tags: ['capital', 'trade'],
          hope: 62,
          dread: 28
        },
        beta: {
          name: 'Northwatch',
          status: 'distressed',
          region: 'frontier',
          tags: ['watch', 'frontier'],
          hope: 37,
          dread: 68
        }
      },
      actors: {
        'alpha.mayor': {
          actorId: 'alpha.mayor',
          townId: 'alpha',
          name: 'Mayor Elira Vale',
          role: 'mayor',
          status: 'active'
        },
        'beta.captain': {
          actorId: 'beta.captain',
          townId: 'beta',
          name: 'Captain Tor Ren',
          role: 'captain',
          status: 'active'
        },
        'beta.warden': {
          actorId: 'beta.warden',
          townId: 'beta',
          name: 'Warden Sera Flint',
          role: 'warden',
          status: 'active'
        }
      }
    }
  }, null, 2), 'utf-8')

  const memoryStore = createMemoryStore({ filePath })
  const registry = createWorldRegistryStore({ memoryStore })

  assert.deepEqual(registry.listTowns(), [
    {
      townId: 'alpha',
      name: 'Ironvale',
      status: 'active',
      region: 'lowlands',
      tags: ['capital', 'trade']
    },
    {
      townId: 'beta',
      name: 'Northwatch',
      status: 'distressed',
      region: 'frontier',
      tags: ['frontier', 'watch']
    }
  ])

  assert.deepEqual(registry.getTown('BETA'), {
    townId: 'beta',
    name: 'Northwatch',
    status: 'distressed',
    region: 'frontier',
    tags: ['frontier', 'watch']
  })

  assert.deepEqual(registry.getActor('alpha.mayor'), {
    actorId: 'alpha.mayor',
    townId: 'alpha',
    name: 'Mayor Elira Vale',
    role: 'mayor',
    status: 'active'
  })

  assert.deepEqual(
    registry.listActors({ townId: 'beta' }).map((actor) => actor.actorId),
    ['beta.captain', 'beta.mayor', 'beta.townsfolk', 'beta.warden']
  )

  assert.deepEqual(
    registry.listTownOfficeholders('beta').map((actor) => [actor.role, actor.name]),
    [
      ['mayor', 'Mayor of Northwatch'],
      ['captain', 'Captain Tor Ren'],
      ['warden', 'Warden Sera Flint']
    ]
  )
})

test('world registry normalization preserves stable actor lookup across seeded towns', () => {
  const filePath = createTempMemoryPath()
  fs.writeFileSync(filePath, JSON.stringify({
    world: {
      actors: {
        'gamma.captain': {
          actorId: 'gamma.captain',
          townId: 'gamma',
          role: 'captain',
          name: 'Captain Hale Rowan',
          status: 'active'
        }
      }
    }
  }, null, 2), 'utf-8')

  const memoryStore = createMemoryStore({ filePath })
  const registry = createWorldRegistryStore({ memoryStore })

  assert.deepEqual(registry.getTown('gamma'), {
    townId: 'gamma',
    name: 'Gamma',
    status: 'active',
    region: null,
    tags: []
  })

  assert.deepEqual(
    registry.listTownOfficeholders('gamma').map((actor) => actor.actorId),
    ['gamma.mayor', 'gamma.captain', 'gamma.warden']
  )
  assert.equal(registry.getActor('gamma.townsfolk').name, 'Townsfolk of Gamma')
})
