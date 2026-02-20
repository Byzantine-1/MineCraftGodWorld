const { goals } = require('mineflayer-pathfinder')
const { GoalFollow, GoalNear } = goals
const { createLogger } = require('./logger')

/**
 * @param {number} min
 * @param {number} max
 */
function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min
}

/**
 * @param {any} bot
 * @param {any} entity
 */
function distanceTo(bot, entity) {
  if (!bot?.entity?.position || !entity?.position) return Infinity
  return bot.entity.position.distanceTo(entity.position)
}

/**
 * @param {any} entity
 */
function isHostileMob(entity) {
  if (!entity || entity.type !== 'mob') return false
  const hostile = new Set([
    'zombie', 'skeleton', 'creeper', 'spider', 'witch',
    'enderman', 'slime', 'drowned', 'husk', 'stray',
    'pillager', 'vindicator', 'evoker', 'ravager', 'phantom'
  ])
  return hostile.has(entity.name)
}

/**
 * @param {any} bot
 * @param {number} maxDist
 */
function getNearestHostile(bot, maxDist = 10) {
  const ent = bot.nearestEntity(e => isHostileMob(e))
  if (!ent) return null
  return distanceTo(bot, ent) <= maxDist ? ent : null
}

/**
 * Create autonomy brain for one mineflayer bot.
 * Failure conditions:
 * - calls that depend on unavailable pathfinder/entity are skipped, not thrown.
 * - bot attack/chat errors are logged as warnings.
 *
 * @param {any} bot
 * @param {{
 *   getPartyOn?: () => boolean,
 *   getLeaderName?: () => string | null,
 *   getHomePos?: () => {x: number, y: number, z: number} | null,
 *   tickMs?: number,
 *   followDistance?: number,
 *   wanderRadius?: number,
 *   hostileDetectRadius?: number,
 *   hostileEngageDistance?: number,
 *   allowAttack?: boolean,
 *   retreatIfLowHP?: boolean,
 *   lowHpThreshold?: number,
 *   logger?: ReturnType<typeof createLogger>
 * }} opts
 */
function createBrain(bot, opts = {}) {
  const getPartyOn = opts.getPartyOn || (() => false)
  const getLeaderName = opts.getLeaderName || (() => null)
  const getHomePos = opts.getHomePos || (() => null)
  const tickMs = Number(opts.tickMs || 2000)
  const followDistance = Number(opts.followDistance || 2)
  const wanderRadius = Number(opts.wanderRadius || 14)
  const hostileDetectRadius = Number(opts.hostileDetectRadius || 9)
  const hostileEngageDistance = Number(opts.hostileEngageDistance || 4)
  const allowAttack = opts.allowAttack !== false
  const retreatIfLowHP = opts.retreatIfLowHP !== false
  const lowHpThreshold = Number(opts.lowHpThreshold || 8)
  const logger = opts.logger || createLogger({ component: 'brain' })

  let timer = null
  const state = {
    lastGoalAt: 0,
    lastWanderPickAt: 0,
    lastChatAt: 0,
    lastAttackAt: 0,
    wanderCooldownMs: randInt(9000, 14000),
    chatCooldownMs: randInt(18000, 35000)
  }

  function leaderEntity() {
    const leader = getLeaderName()
    if (!leader) return null
    return bot?.players?.[leader]?.entity || null
  }

  function sayAmbientLine() {
    const lines = [
      "...I'm watching the treeline.",
      'The air feels wrong tonight.',
      'We should keep moving.',
      'Stay sharp.',
      "I don't like how quiet it is."
    ]
    try {
      bot.chat(lines[randInt(0, lines.length - 1)])
    } catch (err) {
      logger.warn('brain_chat_failed', { bot: bot?.username, error: err instanceof Error ? err.message : String(err) })
    }
  }

  function setGoalThrottled(goal, dynamic = true) {
    if (Date.now() - state.lastGoalAt < 1200) return
    bot.pathfinder.setGoal(goal, dynamic)
    state.lastGoalAt = Date.now()
  }

  function doFollowLeader() {
    const leader = leaderEntity()
    if (!leader) return false
    setGoalThrottled(new GoalFollow(leader, followDistance), true)
    return true
  }

  function doWander() {
    const home = getHomePos()
    if (!home) return false

    const now = Date.now()
    if (now - state.lastWanderPickAt < state.wanderCooldownMs) return true

    const dx = randInt(-wanderRadius, wanderRadius)
    const dz = randInt(-wanderRadius, wanderRadius)
    const x = Math.floor(home.x + dx)
    const y = Math.floor(home.y)
    const z = Math.floor(home.z + dz)

    setGoalThrottled(new GoalNear(x, y, z, 2), true)
    state.lastWanderPickAt = now
    state.wanderCooldownMs = randInt(9000, 16000)
    return true
  }

  /**
   * @param {any} hostile
   */
  async function doHostileReaction(hostile) {
    const hpLow = bot.health <= lowHpThreshold
    const d = distanceTo(bot, hostile)

    if (retreatIfLowHP && hpLow) {
      const home = getHomePos() || bot.entity.position
      setGoalThrottled(new GoalNear(Math.floor(home.x), Math.floor(home.y), Math.floor(home.z), 3), true)
      return true
    }

    if (allowAttack && d <= hostileEngageDistance) {
      if (Date.now() - state.lastAttackAt < 1500) return true
      try {
        await bot.attack(hostile)
        state.lastAttackAt = Date.now()
      } catch (err) {
        logger.warn('brain_attack_failed', { bot: bot?.username, error: err instanceof Error ? err.message : String(err) })
      }
      return true
    }

    return false
  }

  function maybeChat() {
    const now = Date.now()
    if (now - state.lastChatAt < state.chatCooldownMs) return
    if (bot.pathfinder?.isMoving?.()) return
    sayAmbientLine()
    state.lastChatAt = now
    state.chatCooldownMs = randInt(20000, 42000)
  }

  async function tick() {
    if (!bot?.entity || !bot?.pathfinder) return

    const hostile = getNearestHostile(bot, hostileDetectRadius)
    if (hostile) {
      const handled = await doHostileReaction(hostile)
      if (handled) return
    }

    if (getPartyOn()) {
      if (!doFollowLeader()) doWander()
    } else {
      doWander()
    }

    maybeChat()
  }

  function start() {
    if (timer) return
    timer = setInterval(() => {
      tick().catch(err => {
        logger.errorWithStack('brain_tick_failed', err, { bot: bot?.username })
      })
    }, tickMs)
    logger.info('brain_started', { bot: bot?.username, tickMs })
  }

  function stop() {
    if (!timer) return
    clearInterval(timer)
    timer = null
    logger.info('brain_stopped', { bot: bot?.username })
  }

  return { start, stop }
}

module.exports = { createBrain, isHostileMob, getNearestHostile }
