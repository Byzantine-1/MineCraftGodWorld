const { createHash } = require('crypto')

const { AppError } = require('./errors')
const { createExecutionStore } = require('./executionStore')
const { createAuthoritativeSnapshotProjection } = require('./worldSnapshotProjection')

const EXECUTION_HANDOFF_SCHEMA = 'execution-handoff.v1'
const EXECUTION_RESULT_TYPE = 'execution-result.v1'
const EXECUTION_RESULT_SCHEMA_VERSION = 1
const EXECUTION_STATUS_SET = new Set([
  'executed',
  'rejected',
  'stale',
  'duplicate',
  'failed'
])
const HASH_PATTERN = /^[0-9a-f]{64}$/i
const HANDOFF_ID_PATTERN = /^handoff_[0-9a-f]{64}$/i
const RESULT_ID_PATTERN = /^result_[0-9a-f]{64}$/i
const PROPOSAL_ID_PATTERN = /^proposal_[0-9a-f]{64}$/i
const DEFAULT_SALVAGE_FOCUS_MAP = Object.freeze({
  scarcity: 'ruined_hamlet_supplies',
  dread: 'abandoned_shrine_relics',
  general: 'no_mans_land_scrap'
})
const DEFAULT_TALK_TYPE_MAP = Object.freeze({
  'morale-boost': 'gate_warden',
  casual: 'miller'
})

function hasOwn(value, key) {
  return Object.prototype.hasOwnProperty.call(value, key)
}

function isPlainObject(value) {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value))
}

function isNonEmptyString(value) {
  return typeof value === 'string' && value.trim().length > 0
}

function normalizeStringMap(input) {
  const out = {}
  if (!isPlainObject(input)) {
    return out
  }

  for (const [rawKey, rawValue] of Object.entries(input)) {
    if (!isNonEmptyString(rawKey) || !isNonEmptyString(rawValue)) {
      continue
    }
    out[String(rawKey).trim().toLowerCase()] = String(rawValue).trim()
  }
  return out
}

function stableStringify(value) {
  if (Array.isArray(value)) {
    return `[${value.map((entry) => stableStringify(entry)).join(',')}]`
  }

  if (isPlainObject(value)) {
    const keys = Object.keys(value)
      .filter((key) => value[key] !== undefined)
      .sort()
    return `{${keys.map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`).join(',')}}`
  }

  return JSON.stringify(value)
}

function hashValue(value) {
  return createHash('sha256').update(stableStringify(value)).digest('hex')
}

function buildFailure(kind, detail) {
  return {
    kind,
    detail
  }
}

function isValidFailure(entry) {
  return (
    isPlainObject(entry) &&
    isNonEmptyString(entry.kind) &&
    isNonEmptyString(entry.detail)
  )
}

function isValidExecutionState(result) {
  if (!EXECUTION_STATUS_SET.has(result.status)) return false
  if (typeof result.accepted !== 'boolean' || typeof result.executed !== 'boolean') return false
  if (!isNonEmptyString(result.reasonCode)) return false

  if (result.executed && !result.accepted) return false
  if (result.status === 'executed' && (!result.accepted || !result.executed)) return false
  if (result.status === 'failed' && (!result.accepted || result.executed)) return false
  if (['rejected', 'stale', 'duplicate'].includes(result.status) && (result.accepted || result.executed)) {
    return false
  }

  return true
}

function isValidEvaluationBlock(value) {
  if (!isPlainObject(value)) return false

  const preconditions = value.preconditions
  if (!isPlainObject(preconditions)) return false
  if (typeof preconditions.evaluated !== 'boolean' || typeof preconditions.passed !== 'boolean') return false
  if (!Array.isArray(preconditions.failures) || !preconditions.failures.every(isValidFailure)) return false

  const staleCheck = value.staleCheck
  if (!isPlainObject(staleCheck)) return false
  if (typeof staleCheck.evaluated !== 'boolean' || typeof staleCheck.passed !== 'boolean') return false
  if (
    staleCheck.actualSnapshotHash !== null &&
    (!isNonEmptyString(staleCheck.actualSnapshotHash) || !HASH_PATTERN.test(staleCheck.actualSnapshotHash))
  ) {
    return false
  }
  if (
    staleCheck.actualDecisionEpoch !== null &&
    (!Number.isInteger(staleCheck.actualDecisionEpoch) || staleCheck.actualDecisionEpoch < 0)
  ) {
    return false
  }

  const duplicateCheck = value.duplicateCheck
  if (!isPlainObject(duplicateCheck)) return false
  if (typeof duplicateCheck.evaluated !== 'boolean' || typeof duplicateCheck.duplicate !== 'boolean') return false
  if (
    duplicateCheck.duplicateOf !== null &&
    !isNonEmptyString(duplicateCheck.duplicateOf)
  ) {
    return false
  }

  return true
}

function isValidWorldState(worldState) {
  if (worldState === undefined) return true
  if (!isPlainObject(worldState)) return false
  if (
    worldState.postExecutionSnapshotHash !== null &&
    (!isNonEmptyString(worldState.postExecutionSnapshotHash) || !HASH_PATTERN.test(worldState.postExecutionSnapshotHash))
  ) {
    return false
  }
  if (
    worldState.postExecutionDecisionEpoch !== null &&
    (!Number.isInteger(worldState.postExecutionDecisionEpoch) || worldState.postExecutionDecisionEpoch < 0)
  ) {
    return false
  }
  return true
}

function isValidEmbodimentBlock(embodiment) {
  if (embodiment === undefined) return true
  if (!isPlainObject(embodiment)) return false
  if (hasOwn(embodiment, 'backendHint') && embodiment.backendHint !== null && !isNonEmptyString(embodiment.backendHint)) {
    return false
  }
  if (hasOwn(embodiment, 'actions')) {
    if (!Array.isArray(embodiment.actions)) return false
    if (!embodiment.actions.every((entry) => isPlainObject(entry))) return false
  }
  return true
}

function isValidExecutionHandoff(handoff) {
  if (!isPlainObject(handoff)) return false
  if (handoff.schemaVersion !== EXECUTION_HANDOFF_SCHEMA) return false
  if (handoff.advisory !== true) return false
  if (!isNonEmptyString(handoff.handoffId) || !HANDOFF_ID_PATTERN.test(handoff.handoffId)) return false
  if (!isNonEmptyString(handoff.proposalId) || !PROPOSAL_ID_PATTERN.test(handoff.proposalId)) return false
  if (handoff.idempotencyKey !== handoff.proposalId) return false
  if (!isNonEmptyString(handoff.snapshotHash) || !HASH_PATTERN.test(handoff.snapshotHash)) return false
  if (!Number.isInteger(handoff.decisionEpoch) || handoff.decisionEpoch < 0) return false
  if (!isNonEmptyString(handoff.command)) return false
  if (!isPlainObject(handoff.proposal)) return false
  if (!isNonEmptyString(handoff.proposal.type)) return false
  if (!isNonEmptyString(handoff.proposal.actorId)) return false
  if (!isNonEmptyString(handoff.proposal.townId)) return false
  if (!isPlainObject(handoff.proposal.args)) return false
  if (!isPlainObject(handoff.executionRequirements)) return false
  if (handoff.executionRequirements.expectedSnapshotHash !== handoff.snapshotHash) return false
  if (handoff.executionRequirements.expectedDecisionEpoch !== handoff.decisionEpoch) return false
  if (!Array.isArray(handoff.executionRequirements.preconditions)) return false
  return true
}

function parseExecutionHandoffLine(line) {
  if (typeof line !== 'string') {
    return null
  }

  const trimmed = line.trim()
  if (!trimmed.startsWith('{')) {
    return null
  }

  let parsed
  try {
    parsed = JSON.parse(trimmed)
  } catch {
    return null
  }

  if (parsed?.schemaVersion !== EXECUTION_HANDOFF_SCHEMA) {
    return null
  }

  return isValidExecutionHandoff(parsed) ? parsed : null
}

function classifyEngineReason(reason) {
  const text = isNonEmptyString(reason) ? String(reason).trim() : ''
  const lower = text.toLowerCase()

  if (!text) return 'ENGINE_REJECTED'
  if (lower === 'duplicate operation ignored.') return 'DUPLICATE_HANDOFF'
  if (lower === 'unknown town.') return 'UNKNOWN_TOWN'
  if (lower === 'unknown project.') return 'UNKNOWN_PROJECT'
  if (lower === 'unknown salvage target.') return 'UNKNOWN_SALVAGE_TARGET'
  if (lower === 'major mission already active.') return 'MAJOR_MISSION_ALREADY_ACTIVE'
  if (lower === 'no major mission briefing is available. talk to the mayor first.') {
    return 'MAYOR_BRIEFING_REQUIRED'
  }
  if (lower.startsWith('mayor cooldown active until day ')) return 'MAYOR_COOLDOWN_ACTIVE'

  return text
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '') || 'ENGINE_REJECTED'
}

function createExecutionResult({
  handoff,
  proposalType,
  actorId,
  townId,
  authorityCommands,
  status,
  accepted,
  executed,
  reasonCode,
  evaluation,
  worldState,
  embodiment
}) {
  const result = {
    type: EXECUTION_RESULT_TYPE,
    schemaVersion: EXECUTION_RESULT_SCHEMA_VERSION,
    executionId: '',
    resultId: '',
    handoffId: handoff.handoffId,
    proposalId: handoff.proposalId,
    idempotencyKey: handoff.idempotencyKey,
    snapshotHash: handoff.snapshotHash,
    decisionEpoch: handoff.decisionEpoch,
    actorId,
    townId,
    proposalType,
    command: handoff.command,
    authorityCommands: Array.isArray(authorityCommands) ? authorityCommands.slice() : [],
    status,
    accepted,
    executed,
    reasonCode,
    evaluation,
    ...(worldState === undefined ? {} : { worldState }),
    ...(embodiment === undefined ? {} : { embodiment })
  }

  if (!isValidExecutionState(result)) {
    throw new Error('Invalid execution result state.')
  }
  if (!isValidEvaluationBlock(result.evaluation)) {
    throw new Error('Invalid execution result evaluation block.')
  }
  if (!isValidWorldState(result.worldState)) {
    throw new Error('Invalid execution result worldState.')
  }
  if (!isValidEmbodimentBlock(result.embodiment)) {
    throw new Error('Invalid execution result embodiment block.')
  }

  const resultId = `result_${hashValue({
    type: result.type,
    schemaVersion: result.schemaVersion,
    handoffId: result.handoffId,
    proposalId: result.proposalId,
    actorId: result.actorId,
    townId: result.townId,
    proposalType: result.proposalType,
    command: result.command,
    authorityCommands: result.authorityCommands,
    status: result.status,
    accepted: result.accepted,
    executed: result.executed,
    reasonCode: result.reasonCode,
    evaluation: result.evaluation,
    worldState: result.worldState,
    embodiment: result.embodiment
  })}`

  result.executionId = resultId
  result.resultId = resultId
  return result
}

function isValidExecutionResult(result) {
  if (!isPlainObject(result)) return false
  if (result.type !== EXECUTION_RESULT_TYPE) return false
  if (result.schemaVersion !== EXECUTION_RESULT_SCHEMA_VERSION) return false
  if (!isNonEmptyString(result.executionId) || !RESULT_ID_PATTERN.test(result.executionId)) return false
  if (result.resultId !== result.executionId) return false
  if (!isNonEmptyString(result.handoffId) || !HANDOFF_ID_PATTERN.test(result.handoffId)) return false
  if (!isNonEmptyString(result.proposalId) || !PROPOSAL_ID_PATTERN.test(result.proposalId)) return false
  if (result.idempotencyKey !== result.proposalId) return false
  if (!isNonEmptyString(result.snapshotHash) || !HASH_PATTERN.test(result.snapshotHash)) return false
  if (!Number.isInteger(result.decisionEpoch) || result.decisionEpoch < 0) return false
  if (!isNonEmptyString(result.actorId)) return false
  if (!isNonEmptyString(result.townId)) return false
  if (!isNonEmptyString(result.proposalType)) return false
  if (!isNonEmptyString(result.command)) return false
  if (!Array.isArray(result.authorityCommands) || !result.authorityCommands.every((entry) => isNonEmptyString(entry))) {
    return false
  }
  if (!isValidExecutionState(result)) return false
  if (!isValidEvaluationBlock(result.evaluation)) return false
  if (!isValidWorldState(result.worldState)) return false
  if (!isValidEmbodimentBlock(result.embodiment)) return false

  const expectedId = `result_${hashValue({
    type: result.type,
    schemaVersion: result.schemaVersion,
    handoffId: result.handoffId,
    proposalId: result.proposalId,
    actorId: result.actorId,
    townId: result.townId,
    proposalType: result.proposalType,
    command: result.command,
    authorityCommands: result.authorityCommands,
    status: result.status,
    accepted: result.accepted,
    executed: result.executed,
    reasonCode: result.reasonCode,
    evaluation: result.evaluation,
    worldState: result.worldState,
    embodiment: result.embodiment
  })}`

  return result.executionId === expectedId
}

function createTranslation(handoff, world, options) {
  const proposal = handoff.proposal
  const failures = []
  const resolvedTownId = options.resolveTownId(proposal.townId)
  const authorityCommands = []

  if (!isNonEmptyString(resolvedTownId) || !hasOwn(world.towns || {}, resolvedTownId)) {
    failures.push(buildFailure('town_exists', `Unknown authoritative town: ${proposal.townId}`))
  }

  if (proposal.type === 'MAYOR_ACCEPT_MISSION') {
    if (!isNonEmptyString(proposal.args?.missionId)) {
      failures.push(buildFailure('mission_id_present', 'Missing advisory missionId.'))
    }
    if (resolvedTownId && hasOwn(world.towns || {}, resolvedTownId)) {
      const activeMajorMissionId = world.towns[resolvedTownId]?.activeMajorMissionId || null
      if (activeMajorMissionId) {
        failures.push(buildFailure('mission_absent', `Major mission already active for ${resolvedTownId}.`))
      }
    }
    authorityCommands.push(`mayor talk ${resolvedTownId}`, `mayor accept ${resolvedTownId}`)
  } else if (proposal.type === 'PROJECT_ADVANCE') {
    const projectId = proposal.args?.projectId
    if (!isNonEmptyString(projectId)) {
      failures.push(buildFailure('project_exists', 'Missing projectId.'))
    } else if (!Array.isArray(world.projects) || !world.projects.some((project) => String(project?.id || '') === projectId)) {
      failures.push(buildFailure('project_exists', `Unknown project: ${projectId}`))
    } else {
      authorityCommands.push(`project advance ${resolvedTownId} ${projectId}`)
    }
  } else if (proposal.type === 'SALVAGE_PLAN') {
    const focus = String(proposal.args?.focus || '')
    const targetKey = options.salvageFocusMap[focus]
    if (!targetKey) {
      failures.push(buildFailure('salvage_focus_supported', `Unsupported salvage focus: ${focus || '(empty)'}`))
    } else {
      authorityCommands.push(`salvage plan ${resolvedTownId} ${targetKey}`)
    }
  } else if (proposal.type === 'TOWNSFOLK_TALK') {
    const talkType = String(proposal.args?.talkType || '')
    const npcKey = options.talkTypeMap[talkType]
    if (!npcKey) {
      failures.push(buildFailure('talk_type_supported', `Unsupported talk type: ${talkType || '(empty)'}`))
    } else {
      authorityCommands.push(`townsfolk talk ${resolvedTownId} ${npcKey}`)
    }
  } else {
    failures.push(buildFailure('proposal_type_supported', `Unsupported proposal type: ${proposal.type}`))
  }

  return {
    proposalType: proposal.type,
    actorId: proposal.actorId,
    townId: resolvedTownId || proposal.townId,
    authorityCommands,
    failures
  }
}

function createPreconditionEvaluation(failures) {
  return {
    evaluated: true,
    passed: failures.length === 0,
    failures
  }
}

function createDuplicateEvaluation({ actualSnapshotHash, actualDecisionEpoch, duplicateOf }) {
  return {
    preconditions: {
      evaluated: false,
      passed: false,
      failures: []
    },
    staleCheck: {
      evaluated: false,
      passed: false,
      actualSnapshotHash: actualSnapshotHash || null,
      actualDecisionEpoch: Number.isInteger(actualDecisionEpoch) ? actualDecisionEpoch : null
    },
    duplicateCheck: {
      evaluated: true,
      duplicate: true,
      duplicateOf: duplicateOf || null
    }
  }
}

function createWorldStateFromProjection(projection) {
  return {
    postExecutionSnapshotHash: projection?.snapshotHash || null,
    postExecutionDecisionEpoch: Number.isInteger(projection?.decisionEpoch) && projection.decisionEpoch >= 0
      ? projection.decisionEpoch
      : null
  }
}

function createExecutionAdapter(deps) {
  if (!deps?.godCommandService || typeof deps.godCommandService.applyGodCommand !== 'function') {
    throw new AppError({
      code: 'EXECUTION_ADAPTER_CONFIG_ERROR',
      message: 'godCommandService dependency is required.',
      recoverable: false
    })
  }
  if (!deps?.memoryStore || typeof deps.memoryStore.recallWorld !== 'function') {
    throw new AppError({
      code: 'EXECUTION_ADAPTER_CONFIG_ERROR',
      message: 'memoryStore dependency is required.',
      recoverable: false
    })
  }

  const logger = deps.logger || { info: () => {}, warn: () => {} }
  const executionStore = deps.executionStore || createExecutionStore({
    memoryStore: deps.memoryStore,
    logger: typeof logger.child === 'function'
      ? logger.child({ subsystem: 'execution_store' })
      : logger
  })
  const townIdAliases = normalizeStringMap(deps.townIdAliases)
  const salvageFocusMap = {
    ...DEFAULT_SALVAGE_FOCUS_MAP,
    ...normalizeStringMap(deps.salvageFocusMap)
  }
  const talkTypeMap = {
    ...DEFAULT_TALK_TYPE_MAP,
    ...normalizeStringMap(deps.talkTypeMap)
  }

  if (
    !executionStore
    || typeof executionStore.findReceipt !== 'function'
    || typeof executionStore.readSnapshotSource !== 'function'
    || typeof executionStore.recordResult !== 'function'
  ) {
    throw new AppError({
      code: 'EXECUTION_ADAPTER_CONFIG_ERROR',
      message: 'executionStore dependency is required.',
      recoverable: false
    })
  }

  function resolveTownId(townId) {
    if (!isNonEmptyString(townId)) {
      return ''
    }
    const normalized = String(townId).trim()
    return townIdAliases[normalized.toLowerCase()] || normalized
  }

  async function executeHandoff({ handoff, agents = [] } = {}) {
    if (!isValidExecutionHandoff(handoff)) {
      throw new AppError({
        code: 'INVALID_EXECUTION_HANDOFF',
        message: 'Invalid execution handoff payload.',
        recoverable: true
      })
    }

    const beforeWorld = executionStore.readSnapshotSource()
    const beforeProjection = createAuthoritativeSnapshotProjection(beforeWorld)
    const actualDecisionEpoch = beforeProjection.decisionEpoch
    const actualSnapshotHash = beforeProjection.snapshotHash
    const translation = createTranslation(handoff, beforeWorld, {
      resolveTownId,
      salvageFocusMap,
      talkTypeMap
    })
    const existingReceipt = executionStore.findReceipt({
      handoffId: handoff.handoffId,
      idempotencyKey: handoff.idempotencyKey
    })

    if (existingReceipt) {
      const duplicateResult = createExecutionResult({
        handoff,
        proposalType: translation.proposalType,
        actorId: translation.actorId,
        townId: translation.townId,
        authorityCommands: translation.authorityCommands,
        status: 'duplicate',
        accepted: false,
        executed: false,
        reasonCode: 'DUPLICATE_HANDOFF',
        evaluation: createDuplicateEvaluation({
          actualSnapshotHash,
          actualDecisionEpoch,
          duplicateOf: existingReceipt.executionId || existingReceipt.handoffId || null
        }),
        worldState: createWorldStateFromProjection(beforeProjection)
      })
      await executionStore.recordResult(duplicateResult, {
        kind: 'duplicate_replayed',
        persistReceipt: false
      })
      return duplicateResult
    }

    const staleReasonCode = actualDecisionEpoch !== handoff.decisionEpoch
      ? 'STALE_DECISION_EPOCH'
      : (actualSnapshotHash !== handoff.snapshotHash ? 'STALE_SNAPSHOT_HASH' : null)

    if (staleReasonCode) {
      const staleResult = createExecutionResult({
        handoff,
        proposalType: translation.proposalType,
        actorId: translation.actorId,
        townId: translation.townId,
        authorityCommands: translation.authorityCommands,
        status: 'stale',
        accepted: false,
        executed: false,
        reasonCode: staleReasonCode,
        evaluation: {
          preconditions: {
            evaluated: false,
            passed: false,
            failures: []
          },
          staleCheck: {
            evaluated: true,
            passed: false,
            actualSnapshotHash,
            actualDecisionEpoch
          },
          duplicateCheck: {
            evaluated: true,
            duplicate: false,
            duplicateOf: null
          }
        },
        worldState: createWorldStateFromProjection(beforeProjection)
      })
      await executionStore.recordResult(staleResult)
      return staleResult
    }

    if (translation.failures.length > 0) {
      const rejectedResult = createExecutionResult({
        handoff,
        proposalType: translation.proposalType,
        actorId: translation.actorId,
        townId: translation.townId,
        authorityCommands: translation.authorityCommands,
        status: 'rejected',
        accepted: false,
        executed: false,
        reasonCode: 'PRECONDITION_FAILED',
        evaluation: {
          preconditions: createPreconditionEvaluation(translation.failures),
          staleCheck: {
            evaluated: true,
            passed: true,
            actualSnapshotHash,
            actualDecisionEpoch
          },
          duplicateCheck: {
            evaluated: true,
            duplicate: false,
            duplicateOf: null
          }
        },
        worldState: createWorldStateFromProjection(beforeProjection)
      })
      await executionStore.recordResult(rejectedResult)
      return rejectedResult
    }

    let accepted = false
    for (let index = 0; index < translation.authorityCommands.length; index += 1) {
      const authorityCommand = translation.authorityCommands[index]
      const operationId = `${handoff.handoffId}:step:${index + 1}`
      const stepResult = await deps.godCommandService.applyGodCommand({
        agents,
        command: authorityCommand,
        operationId
      })

      if (!stepResult.applied) {
        const reasonCode = classifyEngineReason(stepResult.reason)
        if (reasonCode === 'DUPLICATE_HANDOFF') {
          const currentWorld = executionStore.readSnapshotSource()
          const currentProjection = createAuthoritativeSnapshotProjection(currentWorld)
          const duplicateReceipt = executionStore.findReceipt({
            handoffId: handoff.handoffId,
            idempotencyKey: handoff.idempotencyKey
          })
          const duplicateResult = createExecutionResult({
            handoff,
            proposalType: translation.proposalType,
            actorId: translation.actorId,
            townId: translation.townId,
            authorityCommands: translation.authorityCommands,
            status: 'duplicate',
            accepted: false,
            executed: false,
            reasonCode,
            evaluation: createDuplicateEvaluation({
              actualSnapshotHash: currentProjection.snapshotHash,
              actualDecisionEpoch: currentProjection.decisionEpoch,
              duplicateOf: duplicateReceipt?.executionId || handoff.handoffId
            }),
            worldState: createWorldStateFromProjection(currentProjection)
          })
          await executionStore.recordResult(duplicateResult, {
            kind: 'duplicate_step_replayed',
            persistReceipt: false
          })
          return duplicateResult
        }

        logger.warn('execution_adapter_step_rejected', {
          handoffId: handoff.handoffId,
          authorityCommand,
          reason: stepResult.reason || null
        })

        const currentWorld = executionStore.readSnapshotSource()
        const currentProjection = createAuthoritativeSnapshotProjection(currentWorld)
        const rejectedResult = createExecutionResult({
          handoff,
          proposalType: translation.proposalType,
          actorId: translation.actorId,
          townId: translation.townId,
          authorityCommands: translation.authorityCommands,
          status: accepted ? 'failed' : 'rejected',
          accepted,
          executed: false,
          reasonCode,
          evaluation: {
            preconditions: {
              evaluated: true,
              passed: !accepted,
              failures: accepted
                ? [buildFailure('engine_step', `Authority command failed after partial execution: ${authorityCommand}`)]
                : [buildFailure('engine_rejected', String(stepResult.reason || 'Authority command rejected.'))]
            },
            staleCheck: {
              evaluated: true,
              passed: true,
              actualSnapshotHash,
              actualDecisionEpoch
            },
            duplicateCheck: {
              evaluated: true,
              duplicate: false,
              duplicateOf: null
            }
          },
          worldState: createWorldStateFromProjection(currentProjection)
        })
        await executionStore.recordResult(rejectedResult)
        return rejectedResult
      }

      accepted = true
    }

    const afterWorld = executionStore.readSnapshotSource()
    const afterProjection = createAuthoritativeSnapshotProjection(afterWorld)
    const result = createExecutionResult({
      handoff,
      proposalType: translation.proposalType,
      actorId: translation.actorId,
      townId: translation.townId,
      authorityCommands: translation.authorityCommands,
      status: 'executed',
      accepted: true,
      executed: true,
      reasonCode: 'EXECUTED',
      evaluation: {
        preconditions: createPreconditionEvaluation([]),
        staleCheck: {
          evaluated: true,
          passed: true,
          actualSnapshotHash,
          actualDecisionEpoch
        },
        duplicateCheck: {
          evaluated: true,
          duplicate: false,
          duplicateOf: null
        }
      },
      worldState: createWorldStateFromProjection(afterProjection)
    })
    await executionStore.recordResult(result)

    logger.info('execution_adapter_handoff_executed', {
      handoffId: handoff.handoffId,
      proposalType: translation.proposalType,
      executionId: result.executionId,
      authorityCommands: translation.authorityCommands
    })

    return result
  }

  return {
    executeHandoff,
    resolveTownId
  }
}

module.exports = {
  EXECUTION_HANDOFF_SCHEMA,
  EXECUTION_RESULT_SCHEMA_VERSION,
  EXECUTION_RESULT_TYPE,
  createExecutionAdapter,
  isValidExecutionHandoff,
  isValidExecutionResult,
  parseExecutionHandoffLine
}
