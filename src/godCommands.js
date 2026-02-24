const { createLogger } = require('./logger')
const { AppError } = require('./errors')
const { getObservabilitySnapshot } = require('./runtimeMetrics')

const SUPPORTED_GOD_COMMANDS = new Set(['declare_war', 'make_peace', 'bless_people'])
const INTENT_TYPES = new Set(['idle', 'wander', 'follow', 'respond'])
const JOB_ROLES = new Set(['scout', 'guard', 'builder', 'farmer', 'hauler'])
const FEED_MAX_ENTRIES = 200
const QUEST_TYPES = new Set(['trade_n', 'visit_town', 'rumor_task'])
const RUMOR_TASK_KINDS = new Set(['rumor_trade', 'rumor_visit', 'rumor_choice'])
const QUEST_STATES = new Set(['offered', 'accepted', 'in_progress', 'completed', 'cancelled', 'failed'])
const QUEST_ACTIVE_STATES = new Set(['accepted', 'in_progress'])
const QUEST_CANCELABLE_STATES = new Set(['offered', 'accepted', 'in_progress'])
const MAJOR_MISSION_STATUSES = new Set(['teased', 'briefed', 'active', 'completed', 'failed'])
const DEFAULT_TRADE_QUEST_REWARD = 8
const DEFAULT_VISIT_QUEST_REWARD = 5
const RUMOR_KIND_SET = new Set(['grounded', 'supernatural', 'political'])
const DECISION_STATES = new Set(['open', 'chosen', 'expired'])
const TRAIT_NAMES = ['courage', 'greed', 'faith']
const TRAIT_NAME_SET = new Set(TRAIT_NAMES)
const DEFAULT_AGENT_TRAITS = { courage: 1, greed: 1, faith: 1 }
const MAX_AGENT_TITLE_LEN = 32
const MAX_AGENT_TITLE_COUNT = 20
const CLOCK_PHASES = new Set(['day', 'night'])
const CLOCK_SEASONS = new Set(['dawn', 'long_night'])
const STORY_FACTION_NAMES = ['iron_pact', 'veil_church']
const STORY_FACTION_NAME_SET = new Set(STORY_FACTION_NAMES)
const MOOD_LABEL_THRESHOLD = 25
const MOOD_THRESHOLDS = [25, 50, 75]
const EVENT_TYPES = new Set(['festival', 'shortage', 'omen', 'patrol', 'fog', 'tax_day'])
const EVENT_MOD_KEYS = new Set([
  'fear',
  'unrest',
  'prosperity',
  'trade_reward_bonus',
  'visit_reward_bonus',
  'iron_pact_rep_bonus',
  'veil_church_rep_bonus'
])
const EVENT_DECK = ['festival', 'shortage', 'omen', 'patrol', 'fog', 'tax_day']
const EVENT_TYPE_CONFIG = {
  festival: {
    title: 'Festival stalls glow and caravan bells crowd the square.',
    mods: { prosperity: 3, trade_reward_bonus: 1 }
  },
  shortage: {
    title: 'Bread shelves run thin and traders race vanishing stock.',
    mods: { unrest: 2, trade_reward_bonus: 2 }
  },
  omen: {
    title: 'An omen rides above the steeples and prices turn wary.',
    mods: { fear: 2, veil_church_rep_bonus: 1 }
  },
  patrol: {
    title: 'Iron patrols sweep caravan lanes and tighten checkpoints.',
    mods: { fear: -1, iron_pact_rep_bonus: 1 }
  },
  fog: {
    title: 'Fog eats the crossroads and late caravans miss the bells.',
    mods: { fear: 3, visit_reward_bonus: 2 }
  },
  tax_day: {
    title: 'Tax ledgers slam shut and stallkeepers hide their coin.',
    mods: { unrest: 2 }
  }
}
const STORY_FACTION_DEFAULTS = {
  iron_pact: {
    towns: ['alpha'],
    doctrine: 'Order through steel.',
    rivals: ['veil_church']
  },
  veil_church: {
    towns: ['beta'],
    doctrine: 'Truth through shadow.',
    rivals: ['iron_pact']
  }
}
const SEASON_THREAT_RATES = {
  dawn: { nightRise: 5, dayFall: 3 },
  long_night: { nightRise: 8, dayFall: 2 }
}
const RUMOR_TEMPLATE_LIBRARY = {
  grounded: {
    missing_goods: {
      day: 'Warm bread and whispers circle {town}: goods keep vanishing from crates.',
      night: 'Lanterns dim in {town}; shopkeepers count missing sacks by candlelight.'
    },
    dock_counts: {
      day: 'Porters in {town} whisper of carts that arrive half-empty.',
      night: 'Footsteps fade at the docks of {town}; inventories never add up.'
    },
    old_well_east: {
      day: 'An old well east of {town} glints with dropped coin before noon.',
      night: 'Past the east well of {town}, hush traders swap supplies by lamp.'
    },
    ridge_tracks: {
      day: 'Fresh caravan tracks veer toward the ridge north of {town}; profit may follow.',
      night: 'Ridge tracks north of {town} end in churned mud and snapped rope.'
    }
  },
  supernatural: {
    mist_shapes: {
      day: 'Children in {town} sketch pale figures they swear they saw in the fog.',
      night: 'A pale shape slides through the mist outside {town} and vanishes at dawn.'
    },
    relic_prophecy: {
      day: 'A market elder in {town} mutters that an old relic has awakened.',
      night: 'In {town}, a cracked bell rings alone when no hand is near.'
    },
    birch_line_lights: {
      day: 'Near the birch line west of {town}, blue soot marks a hidden fire ring.',
      night: 'Blue lights drift near the birch line west of {town}; escorts sell out fast.'
    },
    ridge_footsteps: {
      day: 'Hunters by {town} report prints on the ridge that point nowhere.',
      night: 'Footsteps circle the ridge above {town}, always one lantern-length away.'
    }
  },
  political: {
    levy_accusations: {
      day: 'Town clerks in {town} trade accusations over who raised the levy.',
      night: 'Behind shuttered windows in {town}, blame passes faster than coin.'
    },
    guild_blame: {
      day: 'Merchants in {town} whisper that guild books hide a second ledger.',
      night: 'In {town}, quiet voices name rivals as traitors to the commons.'
    },
    caravan_manifest: {
      day: 'A torn caravan manifest in {town} points to unpaid crates at the south bridge.',
      night: 'At the south bridge of {town}, guards argue over missing manifests.'
    },
    toll_bridge_books: {
      day: 'Ledger boys in {town} whisper that toll books were rewritten at dawn.',
      night: 'Near the toll gate of {town}, ink-stained pages trade hands in the dark.'
    }
  }
}
const EVENT_TO_AUTO_RUMOR = {
  shortage: {
    kind: 'grounded',
    templateKey: 'missing_goods',
    templateKeys: ['missing_goods', 'dock_counts', 'old_well_east', 'ridge_tracks'],
    severity: 2,
    expiresInDays: 2
  },
  fog: {
    kind: 'supernatural',
    templateKey: 'mist_shapes',
    templateKeys: ['mist_shapes', 'birch_line_lights', 'ridge_footsteps'],
    severity: 2,
    expiresInDays: 2
  },
  omen: {
    kind: 'supernatural',
    templateKey: 'relic_prophecy',
    templateKeys: ['relic_prophecy', 'birch_line_lights'],
    severity: 3,
    expiresInDays: 3
  },
  tax_day: {
    kind: 'political',
    templateKey: 'levy_accusations',
    templateKeys: ['levy_accusations', 'guild_blame', 'caravan_manifest', 'toll_bridge_books'],
    severity: 2,
    expiresInDays: 2
  }
}
const MARKET_GOODS = ['bread', 'iron', 'timber', 'wool', 'lantern_oil', 'herbs']
const MARKET_GOOD_LABELS = {
  bread: 'Bread',
  iron: 'Iron',
  timber: 'Timber',
  wool: 'Wool',
  lantern_oil: 'Lantern Oil',
  herbs: 'Herbs'
}
const EVENT_MARKET_SIGNALS = {
  festival: { hot: ['wool', 'herbs'], cold: ['iron'] },
  shortage: { hot: ['bread', 'lantern_oil'], cold: ['wool'] },
  omen: { hot: ['lantern_oil', 'iron'], cold: ['herbs'] },
  patrol: { hot: ['iron', 'timber'], cold: ['wool'] },
  fog: { hot: ['lantern_oil', 'bread'], cold: ['timber'] },
  tax_day: { hot: ['bread'], cold: ['wool', 'herbs'] }
}
const MARKET_DAY_SIGNALS = [
  { hot: ['timber', 'bread'], cold: ['wool'], tag: 'builders queue at dawn stalls' },
  { hot: ['iron', 'lantern_oil'], cold: ['herbs'], tag: 'night escorts refill kit packs' },
  { hot: ['herbs', 'bread'], cold: ['iron'], tag: 'camp kitchens pay for quick stock' },
  { hot: ['wool', 'herbs'], cold: ['timber'], tag: 'travelers barter for comfort bundles' },
  { hot: ['iron', 'timber'], cold: ['bread'], tag: 'ridge repairs drain tool benches' },
  { hot: ['lantern_oil', 'bread'], cold: ['wool'], tag: 'fog watch posts buy short-run goods' }
]
const CONTRACT_MAX_PER_TOWN_PER_DAY = 2
const CONTRACT_REWARD_MIN = 1
const CONTRACT_REWARD_MAX = 12
const MAJOR_MISSION_PHASE_START = 1
const MAJOR_MISSION_PHASE_MAX = 3
const MAJOR_MISSION_COOLDOWN_DAYS = 2
const MAJOR_MISSION_MAX_PAYLOAD_KEYS = 12
const TOWN_CRIER_QUEUE_MAX_ENTRIES = 40
const MAX_TOWNSFOLK_QUESTS_PER_TOWN = 24
const MAX_NETHER_EVENT_LEDGER_ENTRIES = 120
const MAX_NETHER_EVENT_PAYLOAD_KEYS = 10
const NETHER_MAX_EVENTS_PER_DAY = 2
const NETHER_MODIFIER_KEYS = ['longNight', 'omen', 'scarcity', 'threat']
const NETHER_EVENT_TYPE_ORDER = [
  'CALM_BEFORE_STORM',
  'LONG_NIGHT',
  'OMEN',
  'SCARCITY',
  'THREAT_SURGE'
]
const NETHER_EVENT_CONFIG = {
  LONG_NIGHT: {
    deltas: { longNight: 1, threat: 1 },
    headline: 'The Nether sky lingers and every route feels one watch longer.'
  },
  OMEN: {
    deltas: { omen: 1, threat: 1 },
    headline: 'Ash-sign omens spread and town sentries tighten gate checks.'
  },
  SCARCITY: {
    deltas: { scarcity: 1, threat: 1 },
    headline: 'Nether shortages ripple into caravan ledgers and ration chatter.'
  },
  THREAT_SURGE: {
    deltas: { threat: 2 },
    headline: 'A surge from below rattles road posts and convoy discipline.'
  },
  CALM_BEFORE_STORM: {
    deltas: { longNight: -1, omen: -1, scarcity: -1, threat: -1 },
    headline: 'For one day the roads breathe easier before the next storm.'
  }
}
const CONTRACT_TRADE_TEMPLATES = [
  {
    id: 'bread_basket',
    kind: 'contract_supply',
    title: 'CONTRACT: Bread Basket',
    good: 'bread',
    tradeN: 1,
    rewardBase: 2,
    dayFlavor: 'Bring warm loaves before noon bells.',
    nightFlavor: 'Stock bread for night shifts before gates close.'
  },
  {
    id: 'iron_run',
    kind: 'contract_trade',
    title: 'CONTRACT: Iron Run',
    good: 'iron',
    tradeN: 2,
    rewardBase: 3,
    dayFlavor: 'Move iron lots while forge crews are awake.',
    nightFlavor: 'Move iron under escort; roads are tense.'
  },
  {
    id: 'stone_shipment',
    kind: 'contract_supply',
    title: 'CONTRACT: Stone Shipment',
    good: 'timber',
    tradeN: 2,
    rewardBase: 3,
    dayFlavor: 'Push stone-and-timber loads to active build sites.',
    nightFlavor: 'Deliver build stock before lantern curfew.'
  },
  {
    id: 'wood_planks',
    kind: 'contract_supply',
    title: 'CONTRACT: Wood Planks',
    good: 'timber',
    tradeN: 1,
    rewardBase: 2,
    dayFlavor: 'Bring planks for scaffolds and roof repairs.',
    nightFlavor: 'Bring planks early; crews shut down after dusk.'
  },
  {
    id: 'lantern_supply',
    kind: 'contract_supply',
    title: 'CONTRACT: Lantern Supply',
    good: 'lantern_oil',
    tradeN: 2,
    rewardBase: 4,
    dayFlavor: 'Fill lantern stocks for market watch routes.',
    nightFlavor: 'Top up lamp oil now; dark roads pay best.'
  },
  {
    id: 'coal_run',
    kind: 'contract_trade',
    title: 'CONTRACT: Coal Run',
    good: 'iron',
    tradeN: 2,
    rewardBase: 4,
    dayFlavor: 'Fuel forge lines before caravan dispatch.',
    nightFlavor: 'Rush fuel lots under guard patrols.'
  },
  {
    id: 'herb_satchel',
    kind: 'contract_supply',
    title: 'CONTRACT: Herb Satchel',
    good: 'herbs',
    tradeN: 1,
    rewardBase: 2,
    dayFlavor: 'Bring herb bundles to cooks and healers.',
    nightFlavor: 'Bring herbs before fog rolls through alleys.'
  },
  {
    id: 'wool_fair',
    kind: 'contract_trade',
    title: 'CONTRACT: Wool Fair',
    good: 'wool',
    tradeN: 1,
    rewardBase: 2,
    dayFlavor: 'Restock wool stalls before caravan crowds land.',
    nightFlavor: 'Move wool quickly; festival trade cools after dark.'
  }
]
const CONTRACT_ROUTE_TEMPLATES = [
  {
    id: 'scout_run',
    kind: 'contract_delivery',
    title: 'CONTRACT: Scout Run',
    rewardBase: 3,
    dayFlavor: 'Scout the lane and deliver route papers.',
    nightFlavor: 'Scout lantern posts and deliver sealed papers.'
  },
  {
    id: 'ridge_route',
    kind: 'contract_delivery',
    title: 'CONTRACT: Ridge Route',
    rewardBase: 4,
    dayFlavor: 'Run ridge permits before convoy departure.',
    nightFlavor: 'Run ridge permits with extra caution after dusk.'
  },
  {
    id: 'caravan_relay',
    kind: 'contract_delivery',
    title: 'CONTRACT: Caravan Relay',
    rewardBase: 3,
    dayFlavor: 'Relay manifests between town gates.',
    nightFlavor: 'Relay manifests by lantern and avoid blind turns.'
  },
  {
    id: 'lantern_patrol',
    kind: 'contract_delivery',
    title: 'CONTRACT: Lantern Patrol',
    rewardBase: 4,
    dayFlavor: 'Map safe lantern stops on the route.',
    nightFlavor: 'Map dark gaps and escort points on the route.'
  }
]
const NIGHT_TROUBLE_LANDMARKS = ['east well', 'north ridge', 'birch line', 'south bridge', 'old toll gate']
const DECISION_DEPRECATION_NOTE = 'Deprecated in Trader Mode: decisions are no longer generated; use Contracts + Market Pulse.'
const MAJOR_MISSION_TEMPLATES = [
  {
    id: 'iron_convoy',
    title: 'Iron Convoy',
    teaser: 'Quartermasters whisper that an iron convoy needs a guard.',
    briefing: 'Escort the iron convoy through exposed roads before losses mount.',
    phaseNotes: [
      'Muster escorts at the gate.',
      'Secure the ridge route.',
      'Deliver the convoy to town stores.'
    ]
  },
  {
    id: 'fog_watch',
    title: 'Fog Watch',
    teaser: 'Lantern wardens ask for a fog-watch charter tonight.',
    briefing: 'Light and hold watch posts before fog cuts the caravan lanes.',
    phaseNotes: [
      'Set lantern posts along the main road.',
      'Hold escort relays through the fog belt.',
      'Sweep and reopen the lane network.'
    ]
  },
  {
    id: 'ledger_reckoning',
    title: 'Ledger Reckoning',
    teaser: 'The mayor seeks hands for a tariff-ledger reckoning.',
    briefing: 'Stabilize trade books and recover missing manifests before unrest rises.',
    phaseNotes: [
      'Collect disputed manifests.',
      'Cross-check toll and dock ledgers.',
      'Publish reconciled totals to calm markets.'
    ]
  }
]
const DECISION_TEMPLATE_BY_EVENT = {
  shortage: {
    prompt: 'Storehouses are thinning. Which trader route gets priority?',
    options: [
      { key: 'ration', label: 'Ration Bread', effects: { mood: { unrest: 1, prosperity: -1 }, threat_delta: -1 } },
      { key: 'import', label: 'Hire Caravans', effects: { mood: { prosperity: 1 }, threat_delta: -1, rumor_spawn: { kind: 'grounded', severity: 1, templateKey: 'dock_counts', expiresInDays: 1 } } },
      { key: 'free_market', label: 'Free Market', effects: { mood: { prosperity: 2, unrest: 1 }, threat_delta: 1 } }
    ]
  },
  festival: {
    prompt: 'Festival crowds gather at the gates. Where will funds go?',
    options: [
      { key: 'fund_music', label: 'Fund Music', effects: { mood: { prosperity: 2, fear: -1 } } },
      { key: 'open_stalls', label: 'Open Stalls', effects: { mood: { prosperity: 1, unrest: -1 } } },
      { key: 'hold_feast', label: 'Hold Feast', effects: { mood: { prosperity: 2, unrest: -1 }, threat_delta: -1 } }
    ]
  },
  omen: {
    prompt: 'An omen shadows the steeples. How should traders respond?',
    options: [
      { key: 'heed_omens', label: 'Heed Omens', effects: { mood: { fear: -1 }, rep_delta: { veil_church: 1 }, rumor_spawn: { kind: 'supernatural', severity: 2, templateKey: 'relic_prophecy', expiresInDays: 1 } } },
      { key: 'denounce', label: 'Denounce Fear', effects: { mood: { unrest: 1, fear: -1 }, rep_delta: { veil_church: -1 } } },
      { key: 'investigate', label: 'Send Investigators', effects: { mood: { fear: -1, prosperity: 1 }, threat_delta: -1 } }
    ]
  },
  patrol: {
    prompt: 'Iron patrols report thin borders. What order goes out?',
    options: [
      { key: 'reinforce', label: 'Reinforce Gates', effects: { mood: { fear: -1 }, rep_delta: { iron_pact: 1 }, threat_delta: -1 } },
      { key: 'expand_routes', label: 'Expand Routes', effects: { mood: { prosperity: 1, unrest: 1 } } },
      { key: 'stand_down', label: 'Stand Down', effects: { mood: { prosperity: 1, fear: 1 }, threat_delta: 1 } }
    ]
  },
  fog: {
    prompt: 'Fog blankets the roads tonight. Which order stands?',
    options: [
      { key: 'light_beacons', label: 'Light Beacons', effects: { mood: { fear: -1, prosperity: 1 }, threat_delta: -1 } },
      { key: 'send_scouts', label: 'Send Scouts', effects: { mood: { fear: 1, unrest: 1 }, rumor_spawn: { kind: 'supernatural', severity: 2, templateKey: 'mist_shapes', expiresInDays: 1 } } },
      { key: 'stay_inside', label: 'Curfew Bells', effects: { mood: { fear: -1, unrest: 1 }, threat_delta: -1 } }
    ]
  },
  tax_day: {
    prompt: 'Tax ledgers spark unrest. Which trader plan goes out?',
    options: [
      { key: 'relief', label: 'Grant Relief', effects: { mood: { unrest: -2, prosperity: -1 }, threat_delta: -1 } },
      { key: 'enforce', label: 'Enforce Collections', effects: { mood: { unrest: 1, prosperity: 1 }, threat_delta: 1 } },
      { key: 'blame_outsiders', label: 'Blame Outsiders', effects: { mood: { fear: 1, unrest: 1 }, rumor_spawn: { kind: 'political', severity: 2, templateKey: 'guild_blame', expiresInDays: 1 } } }
    ]
  }
}

/**
 * @param {unknown} value
 * @param {string} fallback
 * @param {number} maxLen
 */
function asText(value, fallback, maxLen) {
  if (typeof value !== 'string') return fallback
  const trimmed = value.trim()
  if (!trimmed) return fallback
  return trimmed.slice(0, maxLen)
}

/**
 * @param {number} n
 * @param {number} min
 * @param {number} max
 */
function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n))
}

/**
 * @param {unknown} item
 */
function isRuntimeAgentShape(item) {
  return !!item
    && typeof item.name === 'string'
    && typeof item.faction === 'string'
}

/**
 * @param {unknown} item
 */
function isLegacyGodAgentShape(item) {
  return isRuntimeAgentShape(item) && typeof item.applyGodCommand === 'function'
}

/**
 * @param {Record<string, unknown>} profile
 */
function normalizeWorldIntent(profile) {
  const source = (profile && typeof profile.world_intent === 'object' && profile.world_intent)
    ? profile.world_intent
    : {}
  const budgets = (source && typeof source.budgets === 'object' && source.budgets)
    ? source.budgets
    : {}
  const intent = asText(source.intent, 'idle', 16)
  return {
    intent: INTENT_TYPES.has(intent) ? intent : 'idle',
    intent_target: asText(source.intent_target, '', 80) || null,
    intent_set_at: Number(source.intent_set_at || 0) || 0,
    last_action: asText(source.last_action, '', 120),
    last_action_at: Number(source.last_action_at || 0) || 0,
    budgets: {
      minute_bucket: Number(budgets.minute_bucket || 0) || 0,
      events_in_min: Number(budgets.events_in_min || 0) || 0
    },
    manual_override: !!source.manual_override,
    frozen: !!source.frozen,
    is_leader: !!source.is_leader
  }
}

/**
 * @param {any} memory
 * @param {string} agentName
 */
function ensureAgentProfile(memory, agentName) {
  if (!memory.agents[agentName]) {
    memory.agents[agentName] = {
      short: [],
      long: [],
      summary: '',
      archive: [],
      recentUtterances: [],
      lastProcessedTime: 0
    }
  }
  if (!memory.agents[agentName].profile || typeof memory.agents[agentName].profile !== 'object') {
    memory.agents[agentName].profile = {}
  }
  return memory.agents[agentName].profile
}

/**
 * @param {unknown} traitsInput
 */
function normalizeAgentTraits(traitsInput) {
  const source = (traitsInput && typeof traitsInput === 'object' && !Array.isArray(traitsInput))
    ? traitsInput
    : {}
  const traits = {}
  for (const traitName of TRAIT_NAMES) {
    const value = Number(source[traitName])
    traits[traitName] = Number.isFinite(value)
      ? clamp(Math.trunc(value), 0, 3)
      : DEFAULT_AGENT_TRAITS[traitName]
  }
  return traits
}

/**
 * @param {unknown} titlesInput
 */
function normalizeAgentTitles(titlesInput) {
  const source = Array.isArray(titlesInput) ? titlesInput : []
  const titles = []
  const seen = new Set()
  for (const rawTitle of source) {
    const title = asText(rawTitle, '', MAX_AGENT_TITLE_LEN)
    if (!title) continue
    const key = title.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key)
    titles.push(title)
    if (titles.length >= MAX_AGENT_TITLE_COUNT) break
  }
  return titles
}

/**
 * @param {unknown} value
 */
function normalizeRumorsCompleted(value) {
  const count = Number(value)
  if (!Number.isInteger(count) || count < 0) return 0
  return count
}

/**
 * @param {any} profile
 */
function ensureAgentStoryProfile(profile) {
  profile.traits = normalizeAgentTraits(profile.traits)
  profile.titles = normalizeAgentTitles(profile.titles)
  profile.rumors_completed = normalizeRumorsCompleted(profile.rumors_completed)
  profile.rep = normalizeAgentRep(profile.rep)
  return profile
}

/**
 * @param {any} memory
 * @param {string} agentName
 * @param {string} title
 * @param {number} at
 * @param {string} idPrefix
 * @param {string | null} town
 */
function grantAgentTitleIfMissing(memory, agentName, title, at, idPrefix, town) {
  const safeAgentName = asText(agentName, '', 80)
  const safeTitle = asText(title, '', MAX_AGENT_TITLE_LEN)
  const safeIdPrefix = asText(idPrefix, '', 200)
  if (!safeAgentName || !safeTitle || !safeIdPrefix) return false
  const profile = ensureAgentStoryProfile(ensureAgentProfile(memory, safeAgentName))
  const titles = normalizeAgentTitles(profile.titles)
  if (titles.some(item => sameText(item, safeTitle, MAX_AGENT_TITLE_LEN))) return false
  titles.push(safeTitle)
  profile.titles = normalizeAgentTitles(titles)
  const townName = asText(town, '', 80) || undefined
  const message = `${safeAgentName} earns the title "${safeTitle}".`
  appendChronicle(memory, {
    id: `${safeIdPrefix}:chronicle:title_award:${safeAgentName.toLowerCase()}:${safeTitle.toLowerCase()}`,
    type: 'title',
    msg: message,
    at,
    town: townName,
    meta: {
      agent: safeAgentName,
      title: safeTitle
    }
  })
  appendNews(memory, {
    id: `${safeIdPrefix}:news:title_award:${safeAgentName.toLowerCase()}:${safeTitle.toLowerCase()}`,
    topic: 'title',
    msg: message,
    at,
    town: townName,
    meta: {
      agent: safeAgentName,
      title: safeTitle
    }
  })
  return true
}

/**
 * @param {any} memory
 * @param {string} agentName
 * @param {number} at
 * @param {string} idPrefix
 * @param {string | null} town
 */
function applyRepThresholdTitleAwards(memory, agentName, at, idPrefix, town) {
  const safeAgentName = asText(agentName, '', 80)
  if (!safeAgentName) return []
  const profile = ensureAgentStoryProfile(ensureAgentProfile(memory, safeAgentName))
  const rep = normalizeAgentRep(profile.rep)
  const awarded = []
  if (Number(rep.iron_pact || 0) >= 5) {
    if (grantAgentTitleIfMissing(memory, safeAgentName, 'Pact Friend', at, `${idPrefix}:pact_friend`, town)) {
      awarded.push('Pact Friend')
    }
  }
  if (Number(rep.veil_church || 0) >= 5) {
    if (grantAgentTitleIfMissing(memory, safeAgentName, 'Veil Initiate', at, `${idPrefix}:veil_initiate`, town)) {
      awarded.push('Veil Initiate')
    }
  }
  return awarded
}

/**
 * @param {unknown[]} agents
 * @param {string} name
 */
function resolveRuntimeAgent(agents, name) {
  const target = asText(name, '', 80).toLowerCase()
  if (!target) return null
  return agents.find(agent => asText(agent?.name, '', 80).toLowerCase() === target) || null
}

/**
 * @param {any} snapshot
 * @param {unknown[]} runtimeAgents
 * @param {string} name
 */
function resolveKnownAgentName(snapshot, runtimeAgents, name) {
  const runtimeAgent = resolveRuntimeAgent(runtimeAgents, name)
  if (runtimeAgent) return runtimeAgent.name
  const target = asText(name, '', 80).toLowerCase()
  if (!target) return null
  for (const agentName of Object.keys(snapshot?.agents || {})) {
    if (asText(agentName, '', 80).toLowerCase() === target) return agentName
  }
  return null
}

/**
 * @param {any} memory
 */
function findCurrentLeader(memory) {
  for (const [name, agent] of Object.entries(memory.agents || {})) {
    const profile = agent && typeof agent.profile === 'object' ? agent.profile : null
    const worldIntent = profile ? normalizeWorldIntent(profile) : null
    if (worldIntent?.is_leader) return name
  }
  return null
}

/**
 * @param {unknown} value
 */
function asNumber(value) {
  const n = Number(value)
  return Number.isFinite(n) ? n : null
}

/**
 * @param {any} world
 */
function ensureWorldMarkers(world) {
  if (!Array.isArray(world.markers)) world.markers = []
  return world.markers
}

/**
 * @param {unknown} entry
 */
function normalizeMarker(entry) {
  if (!entry || typeof entry !== 'object') return null
  const name = asText(entry.name, '', 80)
  const x = asNumber(entry.x)
  const y = asNumber(entry.y)
  const z = asNumber(entry.z)
  const tag = asText(entry.tag, '', 80)
  const createdAt = Number(entry.created_at || 0) || 0
  if (!name || x === null || y === null || z === null) return null
  return {
    name,
    x,
    y,
    z,
    tag,
    created_at: createdAt
  }
}

/**
 * @param {number} value
 */
function fmt(value) {
  return Number(value || 0).toFixed(2)
}

/**
 * @param {any[]} markers
 * @param {string} name
 */
function findMarkerByName(markers, name) {
  const safeName = asText(name, '', 80).toLowerCase()
  if (!safeName) return null
  for (const marker of markers || []) {
    const normalized = normalizeMarker(marker)
    if (!normalized) continue
    if (normalized.name.toLowerCase() === safeName) return normalized
  }
  return null
}

/**
 * @param {unknown} entry
 */
function normalizeOffer(entry) {
  if (!entry || typeof entry !== 'object') return null
  const offerId = asText(entry.offer_id, '', 160)
  const owner = asText(entry.owner, '', 80)
  const side = asText(entry.side, '', 20).toLowerCase()
  const amount = Number(entry.amount)
  const price = Number(entry.price)
  const createdAt = Number(entry.created_at || 0) || 0
  const active = !!entry.active
  if (!offerId || !owner) return null
  if (side !== 'buy' && side !== 'sell') return null
  if (!Number.isInteger(amount) || amount < 0) return null
  if (active && amount <= 0) return null
  if (!Number.isInteger(price) || price <= 0) return null
  return {
    offer_id: offerId,
    owner,
    side,
    amount,
    price,
    created_at: createdAt,
    active
  }
}

/**
 * @param {unknown} entry
 */
function normalizeMarket(entry) {
  if (!entry || typeof entry !== 'object') return null
  const name = asText(entry.name, '', 80)
  const marker = asText(entry.marker, '', 80)
  const createdAt = Number(entry.created_at || 0) || 0
  const offers = Array.isArray(entry.offers)
    ? entry.offers.map(normalizeOffer).filter(Boolean)
    : []
  if (!name) return null
  const market = { name, created_at: createdAt, offers }
  if (marker) market.marker = marker
  return market
}

/**
 * @param {unknown} marketsInput
 */
function normalizeWorldMarkets(marketsInput) {
  return (Array.isArray(marketsInput) ? marketsInput : [])
    .map(normalizeMarket)
    .filter(Boolean)
}

/**
 * @param {any} world
 */
function ensureWorldMarkets(world) {
  world.markets = normalizeWorldMarkets(world.markets)
  return world.markets
}

/**
 * @param {any[]} markets
 * @param {string} name
 */
function findMarketByName(markets, name) {
  const safeName = asText(name, '', 80).toLowerCase()
  if (!safeName) return null
  for (const market of markets || []) {
    const normalized = normalizeMarket(market)
    if (!normalized) continue
    if (normalized.name.toLowerCase() === safeName) return normalized
  }
  return null
}

/**
 * @param {{offers?: unknown[]}} market
 * @param {string} offerId
 */
function findOfferById(market, offerId) {
  const safeOfferId = asText(offerId, '', 160).toLowerCase()
  if (!safeOfferId) return null
  for (const offer of market?.offers || []) {
    const normalized = normalizeOffer(offer)
    if (!normalized) continue
    if (normalized.offer_id.toLowerCase() === safeOfferId) return normalized
  }
  return null
}

/**
 * @param {unknown} economyInput
 */
function normalizeWorldEconomy(economyInput) {
  const source = (economyInput && typeof economyInput === 'object' && !Array.isArray(economyInput))
    ? economyInput
    : {}
  const ledgerSource = (source.ledger && typeof source.ledger === 'object' && !Array.isArray(source.ledger))
    ? source.ledger
    : {}
  const ledger = {}
  for (const [agentName, balanceRaw] of Object.entries(ledgerSource)) {
    const safeName = asText(agentName, '', 80)
    if (!safeName) continue
    if (typeof balanceRaw !== 'number' || !Number.isFinite(balanceRaw) || balanceRaw < 0) continue
    ledger[safeName] = balanceRaw
  }
  const economy = {
    currency: 'emerald',
    ledger
  }
  if (typeof source.minted_total === 'number' && Number.isFinite(source.minted_total) && source.minted_total >= 0) {
    economy.minted_total = source.minted_total
  }
  return economy
}

/**
 * @param {any} world
 */
function ensureWorldEconomy(world) {
  world.economy = normalizeWorldEconomy(world.economy)
  return world.economy
}

/**
 * @param {unknown} repInput
 */
function normalizeAgentRep(repInput) {
  const source = (repInput && typeof repInput === 'object' && !Array.isArray(repInput))
    ? repInput
    : {}
  const rep = {}
  for (const [factionRaw, valueRaw] of Object.entries(source)) {
    const faction = asText(factionRaw, '', 80).toLowerCase()
    if (!faction) continue
    const value = Number(valueRaw)
    if (!Number.isInteger(value)) continue
    rep[faction] = value
  }
  return rep
}

/**
 * @param {unknown} townsInput
 */
function normalizeStoryTownNames(townsInput) {
  if (!Array.isArray(townsInput)) return []
  const towns = []
  const seen = new Set()
  for (const item of townsInput) {
    const town = asText(item, '', 80)
    if (!town) continue
    const key = town.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key)
    towns.push(town)
  }
  towns.sort((a, b) => a.localeCompare(b))
  return towns
}

/**
 * @param {unknown} rivalsInput
 * @param {string} factionName
 */
function normalizeStoryRivalNames(rivalsInput, factionName) {
  const fallback = STORY_FACTION_DEFAULTS[factionName]?.rivals || []
  const source = Array.isArray(rivalsInput) ? rivalsInput : fallback
  const rivals = []
  const seen = new Set()
  for (const item of source) {
    const rival = asText(item, '', 80).toLowerCase()
    if (!rival || rival === factionName) continue
    if (!STORY_FACTION_NAME_SET.has(rival)) continue
    if (seen.has(rival)) continue
    seen.add(rival)
    rivals.push(rival)
  }
  if (rivals.length === 0) return [...fallback]
  return rivals
}

/**
 * @param {unknown} factionsInput
 */
function normalizeWorldStoryFactions(factionsInput) {
  const source = (factionsInput && typeof factionsInput === 'object' && !Array.isArray(factionsInput))
    ? factionsInput
    : {}
  const factions = {}

  for (const [nameRaw, entryRaw] of Object.entries(source)) {
    const name = asText(nameRaw, '', 80)
    if (!name || !entryRaw || typeof entryRaw !== 'object' || Array.isArray(entryRaw)) continue
    const entry = { ...entryRaw }
    const hostility = Number(entryRaw.hostilityToPlayer)
    const stability = Number(entryRaw.stability)
    if (Number.isFinite(hostility)) entry.hostilityToPlayer = clamp(Math.trunc(hostility), 0, 100)
    if (Number.isFinite(stability)) entry.stability = clamp(Math.trunc(stability), 0, 100)
    factions[name] = entry
  }

  for (const factionName of STORY_FACTION_NAMES) {
    const defaults = STORY_FACTION_DEFAULTS[factionName]
    const raw = (source[factionName] && typeof source[factionName] === 'object' && !Array.isArray(source[factionName]))
      ? source[factionName]
      : {}
    const towns = normalizeStoryTownNames(raw.towns)
    const hostility = Number(raw.hostilityToPlayer)
    const stability = Number(raw.stability)
    const entry = {
      ...raw,
      name: factionName,
      towns: towns.length > 0 ? towns : [...defaults.towns],
      doctrine: asText(raw.doctrine, defaults.doctrine, 160),
      rivals: normalizeStoryRivalNames(raw.rivals, factionName),
      hostilityToPlayer: Number.isFinite(hostility) ? clamp(Math.trunc(hostility), 0, 100) : 10,
      stability: Number.isFinite(stability) ? clamp(Math.trunc(stability), 0, 100) : 70
    }
    factions[factionName] = entry
  }
  return factions
}

/**
 * @param {any} world
 */
function ensureWorldStoryFactions(world) {
  world.factions = normalizeWorldStoryFactions(world.factions)
  return world.factions
}

/**
 * @param {unknown} clockInput
 */
function normalizeWorldClock(clockInput) {
  const source = (clockInput && typeof clockInput === 'object' && !Array.isArray(clockInput))
    ? clockInput
    : {}
  const day = Number(source.day)
  const phase = asText(source.phase, '', 20).toLowerCase()
  const season = asText(source.season, '', 20).toLowerCase()
  const updatedAt = normalizeIsoDate(source.updated_at) || new Date().toISOString()
  return {
    day: Number.isInteger(day) && day >= 1 ? day : 1,
    phase: CLOCK_PHASES.has(phase) ? phase : 'day',
    season: CLOCK_SEASONS.has(season) ? season : 'dawn',
    updated_at: updatedAt
  }
}

/**
 * @param {any} world
 */
function ensureWorldClock(world) {
  world.clock = normalizeWorldClock(world.clock)
  return world.clock
}

/**
 * @param {unknown} threatInput
 */
function normalizeWorldThreat(threatInput) {
  const source = (threatInput && typeof threatInput === 'object' && !Array.isArray(threatInput))
    ? threatInput
    : {}
  const byTownSource = (source.byTown && typeof source.byTown === 'object' && !Array.isArray(source.byTown))
    ? source.byTown
    : {}
  const byTown = {}
  for (const [townRaw, levelRaw] of Object.entries(byTownSource)) {
    const town = asText(townRaw, '', 80)
    const level = Number(levelRaw)
    if (!town || !Number.isFinite(level)) continue
    byTown[town] = clamp(Math.trunc(level), 0, 100)
  }
  return { byTown }
}

/**
 * @param {any} world
 */
function ensureWorldThreat(world) {
  world.threat = normalizeWorldThreat(world.threat)
  return world.threat
}

/**
 * @param {unknown} moodInput
 */
function normalizeTownMood(moodInput) {
  if (!moodInput || typeof moodInput !== 'object' || Array.isArray(moodInput)) return null
  const fear = Number(moodInput.fear)
  const unrest = Number(moodInput.unrest)
  const prosperity = Number(moodInput.prosperity)
  return {
    fear: Number.isFinite(fear) ? clamp(Math.trunc(fear), 0, 100) : 0,
    unrest: Number.isFinite(unrest) ? clamp(Math.trunc(unrest), 0, 100) : 0,
    prosperity: Number.isFinite(prosperity) ? clamp(Math.trunc(prosperity), 0, 100) : 0
  }
}

/**
 * @param {unknown} moodsInput
 */
function normalizeWorldMoods(moodsInput) {
  const source = (moodsInput && typeof moodsInput === 'object' && !Array.isArray(moodsInput))
    ? moodsInput
    : {}
  const byTownSource = (source.byTown && typeof source.byTown === 'object' && !Array.isArray(source.byTown))
    ? source.byTown
    : {}
  const byTown = {}
  for (const [townRaw, moodRaw] of Object.entries(byTownSource)) {
    const townName = asText(townRaw, '', 80)
    if (!townName) continue
    const mood = normalizeTownMood(moodRaw)
    if (!mood) continue
    byTown[townName] = mood
  }
  return { byTown }
}

/**
 * @param {any} world
 */
function ensureWorldMoods(world) {
  world.moods = normalizeWorldMoods(world.moods)
  return world.moods
}

function freshTownMood() {
  return { fear: 0, unrest: 0, prosperity: 0 }
}

/**
 * @param {{fear: number, unrest: number, prosperity: number}} mood
 */
function deriveDominantMoodLabel(mood) {
  const normalized = normalizeTownMood(mood) || freshTownMood()
  const pairs = [
    ['fearful', normalized.fear],
    ['unrestful', normalized.unrest],
    ['prosperous', normalized.prosperity]
  ]
  const maxValue = Math.max(...pairs.map(([, value]) => value))
  if (maxValue < MOOD_LABEL_THRESHOLD) return 'steady'
  const leaders = pairs.filter(([, value]) => value === maxValue)
  if (leaders.length !== 1) return 'steady'
  return leaders[0][0]
}

/**
 * @param {{fear: number, unrest: number, prosperity: number}} moodBefore
 * @param {{fear: number, unrest: number, prosperity: number}} moodAfter
 */
function collectMoodThresholdCrossings(moodBefore, moodAfter) {
  const before = normalizeTownMood(moodBefore) || freshTownMood()
  const after = normalizeTownMood(moodAfter) || freshTownMood()
  const crossings = []
  for (const meter of ['fear', 'unrest', 'prosperity']) {
    for (const threshold of MOOD_THRESHOLDS) {
      const crossedUp = before[meter] < threshold && after[meter] >= threshold
      const crossedDown = before[meter] > threshold && after[meter] <= threshold
      if (!crossedUp && !crossedDown) continue
      crossings.push({ meter, threshold })
    }
  }
  return crossings
}

/**
 * @param {string} townName
 * @param {{fear: number, unrest: number, prosperity: number}} mood
 * @param {string} label
 */
function buildMoodNarration(townName, mood, label) {
  const town = asText(townName, '-', 80) || '-'
  const safeMood = normalizeTownMood(mood) || freshTownMood()
  if (label === 'unrestful') {
    return `[${town}] The streets grow tense. Mood: unrestful (unrest ${safeMood.unrest}).`
  }
  if (label === 'prosperous') {
    return `[${town}] Lanterns glow and laughter returns. Mood: prosperous (prosperity ${safeMood.prosperity}).`
  }
  if (label === 'fearful') {
    return `[${town}] Doors bar early. Mood: fearful (fear ${safeMood.fear}).`
  }
  return `[${town}] The town holds steady. Mood: steady.`
}

/**
 * @param {any} memory
 * @param {{
 *   townName: string | null | undefined,
 *   delta: {fear?: number, unrest?: number, prosperity?: number},
 *   at: number,
 *   idPrefix: string,
 *   reason?: string
 * }} input
 */
function applyTownMoodDelta(memory, input) {
  const townName = asText(input?.townName, '-', 80) || '-'
  const moods = ensureWorldMoods(memory.world)
  const existing = normalizeTownMood(moods.byTown[townName]) || freshTownMood()
  const before = { ...existing }
  const deltaFear = Number.isFinite(Number(input?.delta?.fear)) ? Math.trunc(Number(input.delta.fear)) : 0
  const deltaUnrest = Number.isFinite(Number(input?.delta?.unrest)) ? Math.trunc(Number(input.delta.unrest)) : 0
  const deltaProsperity = Number.isFinite(Number(input?.delta?.prosperity)) ? Math.trunc(Number(input.delta.prosperity)) : 0
  const next = {
    fear: clamp(before.fear + deltaFear, 0, 100),
    unrest: clamp(before.unrest + deltaUnrest, 0, 100),
    prosperity: clamp(before.prosperity + deltaProsperity, 0, 100)
  }
  moods.byTown[townName] = next

  const beforeLabel = deriveDominantMoodLabel(before)
  const afterLabel = deriveDominantMoodLabel(next)
  const thresholdCrossings = collectMoodThresholdCrossings(before, next)
  const shouldNarrate = beforeLabel !== afterLabel || thresholdCrossings.length > 0
  const safeIdPrefix = asText(input?.idPrefix, '', 200)
  const at = Number.isFinite(Number(input?.at)) ? Number(input.at) : Date.now()
  if (shouldNarrate && safeIdPrefix) {
    const message = buildMoodNarration(townName, next, afterLabel)
    appendChronicle(memory, {
      id: `${safeIdPrefix}:chronicle:mood:${townName.toLowerCase()}`,
      type: 'mood',
      msg: message,
      at,
      town: townName,
      meta: {
        fear: next.fear,
        unrest: next.unrest,
        prosperity: next.prosperity,
        mood: afterLabel,
        reason: asText(input?.reason, '', 80) || ''
      }
    })
    appendNews(memory, {
      id: `${safeIdPrefix}:news:mood:${townName.toLowerCase()}`,
      topic: 'world',
      msg: message,
      at,
      town: townName,
      meta: {
        fear: next.fear,
        unrest: next.unrest,
        prosperity: next.prosperity,
        mood: afterLabel
      }
    })
  }

  return {
    townName,
    before,
    after: next,
    label: afterLabel,
    shouldNarrate,
    thresholdCrossings
  }
}

/**
 * @param {unknown} modsInput
 */
function normalizeWorldEventMods(modsInput) {
  if (!modsInput || typeof modsInput !== 'object' || Array.isArray(modsInput)) return {}
  const mods = {}
  for (const [keyRaw, valueRaw] of Object.entries(modsInput)) {
    const key = asText(keyRaw, '', 80)
    if (!key || !EVENT_MOD_KEYS.has(key)) continue
    const value = Number(valueRaw)
    if (!Number.isFinite(value)) continue
    const safeValue = Math.trunc(value)
    if (safeValue === 0) continue
    mods[key] = safeValue
  }
  return mods
}

/**
 * @param {unknown} entry
 */
function normalizeWorldEvent(entry) {
  if (!entry || typeof entry !== 'object' || Array.isArray(entry)) return null
  const id = asText(entry.id, '', 200)
  const type = asText(entry.type, '', 40).toLowerCase()
  const town = asText(entry.town, '', 80)
  const startsDay = Number(entry.starts_day)
  const endsDay = Number(entry.ends_day)
  const mods = normalizeWorldEventMods(entry.mods)
  if (!id || !town) return null
  if (!EVENT_TYPES.has(type)) return null
  if (!Number.isInteger(startsDay) || startsDay < 1) return null
  if (!Number.isInteger(endsDay) || endsDay < startsDay) return null
  return {
    id,
    type,
    town,
    starts_day: startsDay,
    ends_day: endsDay,
    mods
  }
}

/**
 * @param {unknown} eventsInput
 */
function normalizeWorldEvents(eventsInput) {
  const source = (eventsInput && typeof eventsInput === 'object' && !Array.isArray(eventsInput))
    ? eventsInput
    : {}
  const seed = Number(source.seed)
  const index = Number(source.index)
  const active = (Array.isArray(source.active) ? source.active : [])
    .map(normalizeWorldEvent)
    .filter(Boolean)
  return {
    seed: Number.isInteger(seed) ? seed : 1337,
    index: Number.isInteger(index) && index >= 0 ? index : 0,
    active
  }
}

/**
 * @param {any} world
 */
function ensureWorldEvents(world) {
  world.events = normalizeWorldEvents(world.events)
  return world.events
}

/**
 * @param {unknown} entry
 */
function normalizeRumor(entry) {
  if (!entry || typeof entry !== 'object' || Array.isArray(entry)) return null
  const id = asText(entry.id, '', 200)
  const town = asText(entry.town, '', 80)
  const text = asText(entry.text, '', 240)
  const kind = asText(entry.kind, '', 20).toLowerCase()
  const severity = Number(entry.severity)
  const startsDay = Number(entry.starts_day)
  const expiresDay = Number(entry.expires_day)
  const createdAt = Number(entry.created_at)
  const spawnedByEventId = asText(entry.spawned_by_event_id, '', 200)
  const resolvedByQuestId = asText(entry.resolved_by_quest_id, '', 200)
  if (!id || !town || !text) return null
  if (!RUMOR_KIND_SET.has(kind)) return null
  if (!Number.isInteger(severity) || severity < 1 || severity > 3) return null
  if (!Number.isInteger(startsDay) || startsDay < 1) return null
  if (!Number.isInteger(expiresDay) || expiresDay < startsDay) return null
  if (!Number.isFinite(createdAt) || createdAt < 0) return null
  const rumor = {
    id,
    town,
    text,
    kind,
    severity,
    starts_day: startsDay,
    expires_day: expiresDay,
    created_at: createdAt
  }
  if (spawnedByEventId) rumor.spawned_by_event_id = spawnedByEventId
  if (resolvedByQuestId) rumor.resolved_by_quest_id = resolvedByQuestId
  return rumor
}

/**
 * @param {unknown} rumorsInput
 */
function normalizeWorldRumors(rumorsInput) {
  return (Array.isArray(rumorsInput) ? rumorsInput : [])
    .map(normalizeRumor)
    .filter(Boolean)
}

/**
 * @param {any} world
 */
function ensureWorldRumors(world) {
  world.rumors = normalizeWorldRumors(world.rumors)
  return world.rumors
}

/**
 * @param {any[]} rumors
 * @param {string} rumorId
 */
function findRumorById(rumors, rumorId) {
  const target = asText(rumorId, '', 200).toLowerCase()
  if (!target) return null
  for (const entry of rumors || []) {
    const rumor = normalizeRumor(entry)
    if (!rumor) continue
    if (rumor.id.toLowerCase() === target) return rumor
  }
  return null
}

/**
 * @param {any[]} rumors
 * @param {string} idPrefix
 * @param {string} townName
 * @param {string} kind
 * @param {number} day
 */
function createRumorId(rumors, idPrefix, townName, kind, day) {
  const used = new Set((rumors || [])
    .map(entry => asText(entry?.id, '', 200).toLowerCase())
    .filter(Boolean))
  const base = asText(
    `r_${shortStableHash(`${idPrefix}:${townName}:${kind}:${day}`)}`,
    `r_${shortStableHash(`${townName}:${kind}:${day}`)}`,
    200
  )
  if (!used.has(base.toLowerCase())) return base
  let suffix = 2
  while (suffix < 10000) {
    const candidate = asText(`${base}-${suffix}`, base, 200)
    if (!used.has(candidate.toLowerCase())) return candidate
    suffix += 1
  }
  return asText(`${base}-${shortStableHash(`${base}:fallback`)}`, base, 200)
}

/**
 * @param {string} text
 * @param {string} townName
 */
function replaceTownToken(text, townName) {
  return asText(text, '', 240).replace(/\{town\}/g, townName)
}

/**
 * @param {{kind: string, templateKey: string, townName: string, phase: string}} input
 */
function renderRumorTemplate(input) {
  const kind = asText(input?.kind, '', 20).toLowerCase()
  const templateKey = asText(input?.templateKey, '', 80).toLowerCase()
  const townName = asText(input?.townName, '-', 80) || '-'
  const phase = asText(input?.phase, 'day', 20).toLowerCase()
  const byKind = RUMOR_TEMPLATE_LIBRARY[kind] || {}
  const template = byKind[templateKey]
  if (!template) return ''
  const base = phase === 'night'
    ? asText(template.night, '', 240)
    : asText(template.day, '', 240)
  return replaceTownToken(base, townName)
}

/**
 * @param {any} memory
 * @param {{
 *   townName: string,
 *   kind: string,
 *   severity: number,
 *   templateKey: string,
 *   expiresInDays?: number | null,
 *   at: number,
 *   idPrefix: string,
 *   spawnedByEventId?: string | null
 * }} input
 */
function spawnWorldRumor(memory, input) {
  const world = memory.world
  const clock = ensureWorldClock(world)
  const rumors = ensureWorldRumors(world)
  const townName = resolveTownName(world, input.townName)
  if (!townName) {
    throw new AppError({
      code: 'UNKNOWN_TOWN',
      message: `Unknown town for rumor: ${input.townName}`,
      recoverable: true
    })
  }
  const kind = asText(input.kind, '', 20).toLowerCase()
  if (!RUMOR_KIND_SET.has(kind)) {
    throw new AppError({
      code: 'INVALID_RUMOR_KIND',
      message: `Invalid rumor kind: ${input.kind}`,
      recoverable: true
    })
  }
  const severity = Number(input.severity)
  if (!Number.isInteger(severity) || severity < 1 || severity > 3) {
    throw new AppError({
      code: 'INVALID_RUMOR_SEVERITY',
      message: `Invalid rumor severity: ${input.severity}`,
      recoverable: true
    })
  }
  const templateKey = asText(input.templateKey, '', 80).toLowerCase()
  const text = renderRumorTemplate({
    kind,
    templateKey,
    townName,
    phase: clock.phase
  })
  if (!text) {
    throw new AppError({
      code: 'UNKNOWN_RUMOR_TEMPLATE',
      message: `Unknown rumor template: ${templateKey}`,
      recoverable: true
    })
  }
  const expiresInDaysRaw = Number(input.expiresInDays)
  const expiresInDays = Number.isInteger(expiresInDaysRaw) && expiresInDaysRaw >= 0
    ? expiresInDaysRaw
    : 1
  const startsDay = clock.day
  const expiresDay = startsDay + expiresInDays
  const idPrefix = asText(input.idPrefix, '', 200)
  const rumorId = createRumorId(rumors, idPrefix, townName, kind, startsDay)
  const createdAt = Number.isFinite(Number(input.at)) ? Number(input.at) : Date.now()
  const rumor = {
    id: rumorId,
    town: townName,
    text,
    kind,
    severity,
    starts_day: startsDay,
    expires_day: expiresDay,
    created_at: createdAt
  }
  const spawnedByEventId = asText(input.spawnedByEventId, '', 200)
  if (spawnedByEventId) rumor.spawned_by_event_id = spawnedByEventId
  rumors.push(rumor)

  const narration = clock.phase === 'night'
    ? `Lanterns dim. A rumor crawls through ${townName}...`
    : `Warm bread and whispers—rumor spreads in ${townName}.`
  const message = `${narration} ${text}`
  appendChronicle(memory, {
    id: `${idPrefix}:chronicle:rumor_spawn:${rumor.id.toLowerCase()}`,
    type: 'rumor_spawn',
    msg: message,
    at: createdAt,
    town: townName,
    meta: {
      rumor_id: rumor.id,
      kind: rumor.kind,
      severity: rumor.severity
    }
  })
  appendNews(memory, {
    id: `${idPrefix}:news:rumor_spawn:${rumor.id.toLowerCase()}`,
    topic: 'rumor',
    msg: message,
    at: createdAt,
    town: townName,
    meta: {
      rumor_id: rumor.id,
      kind: rumor.kind,
      severity: rumor.severity
    }
  })
  return rumor
}

/**
 * @param {any} effectsInput
 */
function normalizeDecisionEffects(effectsInput) {
  if (!effectsInput || typeof effectsInput !== 'object' || Array.isArray(effectsInput)) return null
  const effects = {}
  const moodSource = (effectsInput.mood && typeof effectsInput.mood === 'object' && !Array.isArray(effectsInput.mood))
    ? effectsInput.mood
    : null
  if (moodSource) {
    const mood = {}
    for (const key of ['fear', 'unrest', 'prosperity']) {
      const value = Number(moodSource[key])
      if (!Number.isInteger(value) || value === 0) continue
      mood[key] = value
    }
    if (Object.keys(mood).length > 0) effects.mood = mood
  }
  const threatDelta = Number(effectsInput.threat_delta)
  if (Number.isInteger(threatDelta) && threatDelta !== 0) effects.threat_delta = threatDelta

  const repDeltaSource = (effectsInput.rep_delta && typeof effectsInput.rep_delta === 'object' && !Array.isArray(effectsInput.rep_delta))
    ? effectsInput.rep_delta
    : null
  if (repDeltaSource) {
    const repDelta = {}
    for (const [factionRaw, valueRaw] of Object.entries(repDeltaSource)) {
      const faction = asText(factionRaw, '', 80).toLowerCase()
      const value = Number(valueRaw)
      if (!faction || !Number.isInteger(value) || value === 0) continue
      repDelta[faction] = value
    }
    if (Object.keys(repDelta).length > 0) effects.rep_delta = repDelta
  }

  const rumorSpawnSource = (effectsInput.rumor_spawn && typeof effectsInput.rumor_spawn === 'object' && !Array.isArray(effectsInput.rumor_spawn))
    ? effectsInput.rumor_spawn
    : null
  if (rumorSpawnSource) {
    const rumorKind = asText(rumorSpawnSource.kind, '', 20).toLowerCase()
    const severity = Number(rumorSpawnSource.severity)
    const templateKey = asText(rumorSpawnSource.templateKey, '', 80).toLowerCase()
    const expiresInDays = Number(rumorSpawnSource.expiresInDays)
    if (
      RUMOR_KIND_SET.has(rumorKind)
      && Number.isInteger(severity) && severity >= 1 && severity <= 3
      && templateKey
    ) {
      const rumorSpawn = { kind: rumorKind, severity, templateKey }
      if (Number.isInteger(expiresInDays) && expiresInDays >= 0) {
        rumorSpawn.expiresInDays = expiresInDays
      }
      effects.rumor_spawn = rumorSpawn
    }
  }
  return Object.keys(effects).length > 0 ? effects : null
}

/**
 * @param {any} optionInput
 */
function normalizeDecisionOption(optionInput) {
  if (!optionInput || typeof optionInput !== 'object' || Array.isArray(optionInput)) return null
  const key = asText(optionInput.key, '', 40).toLowerCase()
  const label = asText(optionInput.label, '', 120)
  const effects = normalizeDecisionEffects(optionInput.effects)
  if (!key || !label || !effects) return null
  return { key, label, effects }
}

/**
 * @param {unknown} entry
 */
function normalizeDecision(entry) {
  if (!entry || typeof entry !== 'object' || Array.isArray(entry)) return null
  const id = asText(entry.id, '', 200)
  const town = asText(entry.town, '', 80)
  const eventId = asText(entry.event_id, '', 200)
  const eventType = asText(entry.event_type, '', 40).toLowerCase()
  const prompt = asText(entry.prompt, '', 240)
  const state = asText(entry.state, '', 20).toLowerCase()
  const chosenKey = asText(entry.chosen_key, '', 40).toLowerCase()
  const startsDay = Number(entry.starts_day)
  const expiresDay = Number(entry.expires_day)
  const createdAt = Number(entry.created_at)
  if (!id || !town || !eventId || !prompt) return null
  if (!EVENT_TYPES.has(eventType)) return null
  if (!DECISION_STATES.has(state)) return null
  if (!Number.isInteger(startsDay) || startsDay < 1) return null
  if (!Number.isInteger(expiresDay) || expiresDay < startsDay) return null
  if (!Number.isFinite(createdAt) || createdAt < 0) return null
  const optionsRaw = Array.isArray(entry.options) ? entry.options : []
  const options = []
  const seen = new Set()
  for (const rawOption of optionsRaw) {
    const option = normalizeDecisionOption(rawOption)
    if (!option) continue
    if (seen.has(option.key)) continue
    seen.add(option.key)
    options.push(option)
    if (options.length >= 3) break
  }
  if (options.length < 2) return null
  if (state === 'chosen') {
    if (!chosenKey || !seen.has(chosenKey)) return null
  }
  const decision = {
    id,
    town,
    event_id: eventId,
    event_type: eventType,
    prompt,
    options,
    state,
    starts_day: startsDay,
    expires_day: expiresDay,
    created_at: createdAt
  }
  if (chosenKey) decision.chosen_key = chosenKey
  return decision
}

/**
 * @param {unknown} decisionsInput
 */
function normalizeWorldDecisions(decisionsInput) {
  return (Array.isArray(decisionsInput) ? decisionsInput : [])
    .map(normalizeDecision)
    .filter(Boolean)
}

/**
 * @param {any} world
 */
function ensureWorldDecisions(world) {
  world.decisions = normalizeWorldDecisions(world.decisions)
  return world.decisions
}

/**
 * @param {any[]} decisions
 * @param {string} decisionId
 */
function findDecisionById(decisions, decisionId) {
  const target = asText(decisionId, '', 200).toLowerCase()
  if (!target) return null
  for (const entry of decisions || []) {
    const decision = normalizeDecision(entry)
    if (!decision) continue
    if (decision.id.toLowerCase() === target) return decision
  }
  return null
}

/**
 * @param {any[]} decisions
 * @param {string} idPrefix
 * @param {string} townName
 * @param {string} eventId
 */
function createDecisionId(decisions, idPrefix, townName, eventId) {
  const used = new Set((decisions || [])
    .map(entry => asText(entry?.id, '', 200).toLowerCase())
    .filter(Boolean))
  const base = asText(
    `d_${shortStableHash(`${idPrefix}:${townName}:${eventId}`)}`,
    `d_${shortStableHash(`${townName}:${eventId}`)}`,
    200
  )
  if (!used.has(base.toLowerCase())) return base
  let suffix = 2
  while (suffix < 10000) {
    const candidate = asText(`${base}-${suffix}`, base, 200)
    if (!used.has(candidate.toLowerCase())) return candidate
    suffix += 1
  }
  return asText(`${base}-${shortStableHash(`${base}:fallback`)}`, base, 200)
}

/**
 * @param {string} eventType
 */
function buildDecisionTemplate(eventType) {
  const type = asText(eventType, '', 40).toLowerCase()
  const template = DECISION_TEMPLATE_BY_EVENT[type]
  if (!template) return null
  const options = (Array.isArray(template.options) ? template.options : [])
    .map(normalizeDecisionOption)
    .filter(Boolean)
    .slice(0, 3)
  if (options.length < 2) return null
  return {
    prompt: asText(template.prompt, '', 240),
    options
  }
}

/**
 * @param {any} memory
 * @param {any} event
 * @param {{at: number, idPrefix: string}} input
 */
function createDecisionForEvent(memory, event, input) {
  const normalizedEvent = normalizeWorldEvent(event)
  if (!normalizedEvent) return null
  const template = buildDecisionTemplate(normalizedEvent.type)
  if (!template) return null
  const clock = ensureWorldClock(memory.world)
  const decisions = ensureWorldDecisions(memory.world)
  const idPrefix = asText(input?.idPrefix, '', 200)
  const decisionId = createDecisionId(decisions, idPrefix, normalizedEvent.town, normalizedEvent.id)
  const at = Number.isFinite(Number(input?.at)) ? Number(input.at) : Date.now()
  const decision = {
    id: decisionId,
    town: normalizedEvent.town,
    event_id: normalizedEvent.id,
    event_type: normalizedEvent.type,
    prompt: template.prompt,
    options: template.options,
    state: 'open',
    starts_day: clock.day,
    expires_day: clock.day,
    created_at: at
  }
  decisions.push(decision)
  const message = `[${normalizedEvent.town}] LEGACY DECISION: ${decision.prompt}`
  appendChronicle(memory, {
    id: `${idPrefix}:chronicle:decision_open:${decision.id.toLowerCase()}`,
    type: 'decision_open',
    msg: message,
    at,
    town: normalizedEvent.town,
    meta: {
      decision_id: decision.id,
      event_id: normalizedEvent.id,
      event_type: normalizedEvent.type
    }
  })
  appendNews(memory, {
    id: `${idPrefix}:news:decision_open:${decision.id.toLowerCase()}`,
    topic: 'world',
    msg: message,
    at,
    town: normalizedEvent.town,
    meta: {
      decision_id: decision.id,
      event_id: normalizedEvent.id,
      event_type: normalizedEvent.type
    }
  })
  return decision
}

/**
 * @param {any} event
 * @param {number} day
 */
function isEventActiveForDay(event, day) {
  const normalized = normalizeWorldEvent(event)
  if (!normalized) return false
  if (!Number.isInteger(day) || day < 1) return false
  return normalized.starts_day <= day && normalized.ends_day >= day
}

/**
 * @param {any} world
 * @param {string} townName
 * @param {number} day
 */
function findActiveEventsForTown(world, townName, day) {
  const safeTownName = asText(townName, '', 80)
  if (!safeTownName) return []
  const events = normalizeWorldEvents(world?.events).active
  return events
    .filter(event => sameText(event.town, safeTownName, 80) && isEventActiveForDay(event, day))
    .sort((a, b) => {
      const dayDiff = Number(b.starts_day || 0) - Number(a.starts_day || 0)
      if (dayDiff !== 0) return dayDiff
      return asText(a.id, '', 200).localeCompare(asText(b.id, '', 200))
    })
}

/**
 * @param {any[]} events
 * @param {string} key
 */
function sumEventModifier(events, key) {
  let total = 0
  for (const event of events || []) {
    const mods = normalizeWorldEventMods(event?.mods)
    total += Number(mods[key] || 0)
  }
  return total
}

/**
 * @param {number} level
 */
function deriveThreatBand(level) {
  const safe = clamp(Math.trunc(Number(level) || 0), 0, 100)
  if (safe >= 80) return 'extreme'
  if (safe >= 55) return 'high'
  if (safe >= 30) return 'moderate'
  return 'low'
}

/**
 * @param {string} good
 */
function toMarketGoodLabel(good) {
  return MARKET_GOOD_LABELS[good] || good
}

/**
 * @param {string} goodLabel
 */
function toMarketGoodKey(goodLabel) {
  const key = asText(goodLabel, '', 40).toLowerCase().replace(/\s+/g, '_')
  return MARKET_GOODS.includes(key) ? key : ''
}

/**
 * @param {number} score
 */
function toPulseMultiplierHint(score) {
  const safe = Number(score || 0)
  if (safe >= 6) return 'x1.8'
  if (safe >= 4) return 'x1.5'
  if (safe >= 2) return 'x1.2'
  return 'x1.1'
}

/**
 * @param {Map<string, {score: number, tags: Set<string>}>} scoreByGood
 * @param {string} good
 * @param {number} delta
 * @param {string} tag
 */
function applyMarketSignal(scoreByGood, good, delta, tag) {
  const safeGood = asText(good, '', 40).toLowerCase()
  if (!safeGood || !scoreByGood.has(safeGood)) return
  const entry = scoreByGood.get(safeGood)
  entry.score += Number(delta || 0)
  if (tag) entry.tags.add(asText(tag, '', 80))
}

/**
 * @param {Set<string>} tags
 * @param {string} fallback
 */
function summarizePulseReason(tags, fallback) {
  const items = Array.from(tags || [])
    .map(tag => asText(tag, '', 80))
    .filter(Boolean)
    .slice(0, 2)
  if (items.length === 0) return fallback
  return items.join(' + ')
}

/**
 * @param {'hot' | 'cold'} kind
 * @param {string} goodLabel
 * @param {string} reason
 */
function toActionablePulseReason(kind, goodLabel, reason) {
  const safeGoodLabel = asText(goodLabel, 'goods', 40)
  const safeReason = asText(reason, '', 140)
  if (kind === 'hot') {
    return asText(`Bring ${safeGoodLabel} now; ${safeReason || 'stalls are bidding hard'}.`, `Bring ${safeGoodLabel} now.`, 180)
  }
  return asText(`Hold ${safeGoodLabel} stock; ${safeReason || 'buyers are thin right now'}.`, `Hold ${safeGoodLabel} stock for now.`, 180)
}

/**
 * @param {string} townName
 * @param {number} day
 * @param {string} season
 */
function pickMarketDaySignal(townName, day, season) {
  if (!Array.isArray(MARKET_DAY_SIGNALS) || MARKET_DAY_SIGNALS.length === 0) return null
  const idx = stableHashNumber(`${townName}:${day}:${season}:market_day_signal`) % MARKET_DAY_SIGNALS.length
  return MARKET_DAY_SIGNALS[idx] || null
}

/**
 * @param {number} level
 */
function toCaravanTroubleChance(level) {
  const band = deriveThreatBand(level)
  if (band === 'extreme') return 60
  if (band === 'high') return 38
  if (band === 'moderate') return 18
  return 6
}

/**
 * @param {string} townName
 * @param {number} level
 * @param {number} day
 * @param {string} season
 */
function shouldEmitCaravanTrouble(townName, level, day, season) {
  const chance = toCaravanTroubleChance(level)
  const roll = stableHashNumber(`${townName}:${day}:${season}:caravan_trouble`) % 100
  return roll < chance
}

/**
 * @param {any} world
 * @param {string} townName
 */
function getRouteRisk(townName, world) {
  const resolvedTown = resolveTownName(world, townName) || asText(townName, '-', 80) || '-'
  const clock = normalizeWorldClock(world?.clock)
  const threat = normalizeWorldThreat(world?.threat)
  const moods = normalizeWorldMoods(world?.moods)
  const mood = normalizeTownMood(moods.byTown[resolvedTown]) || freshTownMood()
  const threatLevel = clamp(Math.trunc(Number(threat.byTown[resolvedTown] || 0)), 0, 100)
  const seasonBonus = clock.season === 'long_night' ? 10 : 0
  const phaseBonus = clock.phase === 'night' ? 15 : 0
  const fearWeight = Math.trunc(mood.fear / 2)
  const score = clamp(threatLevel + fearWeight + seasonBonus + phaseBonus, 0, 100)
  const label = deriveThreatBand(score)

  const reasons = []
  if (clock.phase === 'night') reasons.push('avoid ridge lanes after dusk')
  if (clock.season === 'long_night') reasons.push('carry extra lanterns for long_night roads')
  if (threatLevel >= 55) reasons.push(`escort demand is high at threat ${threatLevel}`)
  if (mood.fear >= 40) reasons.push(`fear ${mood.fear} keeps caravans jumpy`)
  const reason = reasons.length > 0
    ? reasons.slice(0, 2).join('; ')
    : `stage guards before departure; threat ${threatLevel} in ${clock.phase}`
  return {
    label,
    reason,
    nightPenaltyHint: 'Night travel runs one risk tier higher; route through lit markers.'
  }
}

/**
 * @param {string} townName
 * @param {any} world
 */
function getMarketPulse(townName, world) {
  const resolvedTown = resolveTownName(world, townName) || asText(townName, '-', 80) || '-'
  const clock = normalizeWorldClock(world?.clock)
  const moodState = normalizeTownMood(normalizeWorldMoods(world?.moods).byTown[resolvedTown]) || freshTownMood()
  const moodLabel = deriveDominantMoodLabel(moodState)
  const threatLevel = clamp(Math.trunc(Number(normalizeWorldThreat(world?.threat).byTown[resolvedTown] || 0)), 0, 100)
  const activeEvents = findActiveEventsForTown(world, resolvedTown, clock.day)
  const activeEventType = asText(activeEvents[0]?.type, '', 40).toLowerCase()
  const scoreByGood = new Map()
  for (const good of MARKET_GOODS) {
    scoreByGood.set(good, { score: 0, tags: new Set() })
  }

  if (moodLabel === 'prosperous') {
    applyMarketSignal(scoreByGood, 'wool', 2, 'prosperous crowds buy comfort')
    applyMarketSignal(scoreByGood, 'herbs', 2, 'prosperous kitchens stock flavor')
    applyMarketSignal(scoreByGood, 'iron', -1, 'less urgency for heavy gear')
  } else if (moodLabel === 'fearful') {
    applyMarketSignal(scoreByGood, 'lantern_oil', 2, 'fearful nights demand light')
    applyMarketSignal(scoreByGood, 'bread', 1, 'households stock basics')
    applyMarketSignal(scoreByGood, 'wool', -1, 'luxury stalls cool')
  } else if (moodLabel === 'unrestful') {
    applyMarketSignal(scoreByGood, 'iron', 2, 'unrest hardens demand for tools')
    applyMarketSignal(scoreByGood, 'timber', 1, 'repairs and barricades rise')
    applyMarketSignal(scoreByGood, 'herbs', -1, 'non-essentials slow')
  } else {
    applyMarketSignal(scoreByGood, 'bread', 1, 'steady foot traffic keeps staples moving')
    applyMarketSignal(scoreByGood, 'timber', 1, 'steady crews keep building')
  }

  const eventSignals = EVENT_MARKET_SIGNALS[activeEventType] || null
  if (eventSignals) {
    for (const hotGood of eventSignals.hot || []) {
      applyMarketSignal(scoreByGood, hotGood, 2, `event:${activeEventType}`)
    }
    for (const coldGood of eventSignals.cold || []) {
      applyMarketSignal(scoreByGood, coldGood, -2, `event:${activeEventType}`)
    }
  }

  if (threatLevel >= 70) {
    applyMarketSignal(scoreByGood, 'iron', 2, 'high threat drives fortification')
    applyMarketSignal(scoreByGood, 'lantern_oil', 2, 'high threat extends night watches')
    applyMarketSignal(scoreByGood, 'wool', -1, 'high threat cuts festival trade')
    applyMarketSignal(scoreByGood, 'herbs', -1, 'high threat constrains foraging')
  } else if (threatLevel <= 25) {
    applyMarketSignal(scoreByGood, 'wool', 1, 'safer roads invite market color')
    applyMarketSignal(scoreByGood, 'herbs', 1, 'safer roads reopen herb routes')
    applyMarketSignal(scoreByGood, 'iron', -1, 'safer roads reduce emergency demand')
  }

  if (clock.season === 'long_night') {
    applyMarketSignal(scoreByGood, 'lantern_oil', 1, 'long_night consumes lamp stocks')
    applyMarketSignal(scoreByGood, 'timber', 1, 'long_night needs extra fuel')
  }
  if (clock.phase === 'night') {
    applyMarketSignal(scoreByGood, 'lantern_oil', 1, 'night routes run by lantern')
    applyMarketSignal(scoreByGood, 'bread', 1, 'night shifts stock quick meals')
  }
  const daySignal = pickMarketDaySignal(resolvedTown, clock.day, clock.season)
  if (daySignal) {
    for (const hotGood of daySignal.hot || []) {
      applyMarketSignal(scoreByGood, hotGood, 1, daySignal.tag)
    }
    for (const coldGood of daySignal.cold || []) {
      applyMarketSignal(scoreByGood, coldGood, -1, daySignal.tag)
    }
  }

  const ranked = Array.from(scoreByGood.entries())
    .map(([good, entry]) => ({ good, score: Number(entry.score || 0), tags: entry.tags }))
    .sort((left, right) => {
      const diff = right.score - left.score
      if (diff !== 0) return diff
      return left.good.localeCompare(right.good)
    })
  const hotCandidates = ranked.filter(entry => entry.score >= 1)
  const coldCandidates = ranked.slice().reverse().filter(entry => entry.score <= -1)
  const hotPicked = (hotCandidates.length > 0 ? hotCandidates : ranked.slice(0, 1)).slice(0, 3)
  const hotSet = new Set(hotPicked.map(entry => entry.good))
  const coldPicked = (coldCandidates.length > 0
    ? coldCandidates.filter(entry => !hotSet.has(entry.good))
    : ranked.slice().reverse().filter(entry => !hotSet.has(entry.good)).slice(0, 1))
    .slice(0, 3)

  const hot = hotPicked.map(entry => ({
    good: toMarketGoodLabel(entry.good),
    reason: toActionablePulseReason('hot', toMarketGoodLabel(entry.good), summarizePulseReason(entry.tags, 'tight stalls and fast turnover')),
    multiplierHint: toPulseMultiplierHint(entry.score)
  }))
  const cold = coldPicked.map(entry => ({
    good: toMarketGoodLabel(entry.good),
    reason: toActionablePulseReason('cold', toMarketGoodLabel(entry.good), summarizePulseReason(entry.tags, 'slow shelves and soft bids'))
  }))
  const risk = getRouteRisk(resolvedTown, world)

  return { hot, cold, risk }
}

/**
 * @param {string} townName
 * @param {any} world
 */
function getTraderTip(townName, world) {
  const pulse = getMarketPulse(townName, world)
  const hot = asText(pulse.hot[0]?.good, 'Bread', 40)
  const cold = asText(pulse.cold[0]?.good, 'Wool', 40)
  if (pulse.risk.label === 'extreme' || pulse.risk.label === 'high') {
    return `Move ${hot} in daylight; avoid long routes with ${cold} after dusk.`
  }
  if (pulse.risk.label === 'moderate') {
    return `Run short loops: sell ${hot}, buy ${cold} low before nightfall.`
  }
  return `Press the market: flip ${cold} into ${hot} while roads stay calm.`
}

/**
 * @param {any} quest
 */
function isContractQuest(quest) {
  const normalized = normalizeQuest(quest)
  if (!normalized) return false
  if (normalized.type !== 'trade_n' && normalized.type !== 'visit_town') return false
  return normalized.meta?.contract === true
}

/**
 * @param {any[]} quests
 * @param {string} questId
 */
function findContractQuestById(quests, questId) {
  const quest = findQuestById(quests || [], questId)
  if (!quest) return null
  return isContractQuest(quest) ? quest : null
}

/**
 * @param {any} snapshot
 * @param {unknown[]} runtimeAgents
 */
function resolveDefaultContractOwner(snapshot, runtimeAgents) {
  const runtimeNames = (Array.isArray(runtimeAgents) ? runtimeAgents : [])
    .map(agent => asText(agent?.name, '', 80))
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b))
  if (runtimeNames.length === 0) {
    return Object.keys(snapshot?.agents || {})
      .map(name => asText(name, '', 80))
      .filter(Boolean)
      .sort((a, b) => a.localeCompare(b))[0] || ''
  }
  const mara = runtimeNames.find(name => sameText(name, 'Mara', 80))
  return mara || runtimeNames[0]
}

/**
 * @param {any[]} towns
 * @param {string} townName
 * @param {number} day
 */
function pickContractRouteTarget(towns, townName, day) {
  const others = (towns || [])
    .map(town => asText(town?.townName, '', 80))
    .filter(name => name && !sameText(name, townName, 80))
    .sort((a, b) => a.localeCompare(b))
  if (others.length === 0) return ''
  const idx = stableHashNumber(`${townName}:${day}:route`) % others.length
  return others[idx]
}

/**
 * @param {{id: string}[]} templates
 * @param {string} key
 */
function pickContractTemplate(templates, key) {
  const source = (Array.isArray(templates) ? templates : [])
    .slice()
    .sort((a, b) => asText(a?.id, '', 80).localeCompare(asText(b?.id, '', 80)))
  if (source.length === 0) return null
  const idx = stableHashNumber(key) % source.length
  return source[idx] || null
}

/**
 * @param {string} phase
 * @param {{dayFlavor: string, nightFlavor: string}} template
 */
function toContractFlavor(phase, template) {
  if (phase === 'night') return asText(template?.nightFlavor, '', 120)
  return asText(template?.dayFlavor, '', 120)
}

/**
 * @param {string} townName
 * @param {number} day
 * @param {number} slot
 * @param {string} riskLabel
 * @param {string} hotGood
 */
function pickTradeContractTemplate(townName, day, slot, riskLabel, hotGood) {
  const safeHotGood = asText(hotGood, '', 40)
  const hotPool = CONTRACT_TRADE_TEMPLATES.filter(template => sameText(template.good, safeHotGood, 40))
  const pool = hotPool.length > 0 ? hotPool : CONTRACT_TRADE_TEMPLATES
  return pickContractTemplate(pool, `${townName}:${day}:${slot}:${riskLabel}:${safeHotGood}:trade_template`)
}

/**
 * @param {string} townName
 * @param {number} day
 * @param {number} slot
 * @param {string} riskLabel
 */
function pickRouteContractTemplate(townName, day, slot, riskLabel) {
  return pickContractTemplate(CONTRACT_ROUTE_TEMPLATES, `${townName}:${day}:${slot}:${riskLabel}:route_template`)
}

/**
 * @param {any} world
 * @param {string} townName
 * @param {{townName: string, marker: any}[]} towns
 * @param {number} day
 * @param {number} slot
 */
function buildContractDraft(world, townName, towns, day, slot) {
  const pulse = getMarketPulse(townName, world)
  const risk = getRouteRisk(townName, world)
  const clock = normalizeWorldClock(world?.clock)
  const hotGoodLabel = asText(pulse.hot[0]?.good, 'Bread', 40)
  const hotGoodKey = toMarketGoodKey(hotGoodLabel) || 'bread'
  const markets = normalizeWorldMarkets(world?.markets)
  const town = findTownByName(towns, townName)
  const marketName = markets
    .filter(market => sameText(market.marker, town?.marker?.name, 80))
    .sort((a, b) => a.name.localeCompare(b.name))[0]?.name || ''

  const riskBonus = risk.label === 'extreme'
    ? 4
    : risk.label === 'high'
      ? 3
      : risk.label === 'moderate'
        ? 2
        : 1

  const tradeTemplate = pickTradeContractTemplate(townName, day, slot, risk.label, hotGoodKey) || CONTRACT_TRADE_TEMPLATES[0]
  const tradeNBase = Number.isInteger(tradeTemplate.tradeN) ? tradeTemplate.tradeN : 1
  const tradeN = clamp(tradeNBase, 1, 3)
  const tradeFlavor = toContractFlavor(clock.phase, tradeTemplate)

  if (slot === 0) {
    const desc = marketName
      ? `${tradeFlavor} Close ${tradeN} lot${tradeN === 1 ? '' : 's'} at ${marketName}.`
      : `${tradeFlavor} Close ${tradeN} lot${tradeN === 1 ? '' : 's'} for ${townName}.`
    return {
      kind: asText(tradeTemplate.kind, 'contract_supply', 40),
      type: 'trade_n',
      reward: clamp(Number(tradeTemplate.rewardBase || 2) + riskBonus + (tradeN - 1), CONTRACT_REWARD_MIN, CONTRACT_REWARD_MAX),
      objective: {
        kind: 'trade_n',
        n: tradeN,
        ...(marketName ? { market: marketName } : {})
      },
      progress: { done: 0 },
      title: asText(tradeTemplate.title, 'CONTRACT: Supply Run', 120),
      desc,
      risk,
      hotGood: toMarketGoodLabel(hotGoodKey)
    }
  }

  const routeTown = pickContractRouteTarget(towns, townName, day)
  if (routeTown) {
    const routeTemplate = pickRouteContractTemplate(townName, day, slot, risk.label) || CONTRACT_ROUTE_TEMPLATES[0]
    const routeFlavor = toContractFlavor(clock.phase, routeTemplate)
    return {
      kind: asText(routeTemplate.kind, 'contract_delivery', 40),
      type: 'visit_town',
      reward: clamp(Number(routeTemplate.rewardBase || 3) + riskBonus, CONTRACT_REWARD_MIN, CONTRACT_REWARD_MAX),
      objective: { kind: 'visit_town', town: routeTown },
      progress: { visited: false },
      title: asText(routeTemplate.title, 'CONTRACT: Caravan Route', 120),
      desc: `${routeFlavor} Run from ${townName} to ${routeTown}.`,
      risk,
      hotGood: toMarketGoodLabel(hotGoodKey)
    }
  }

  const fallbackTemplate = pickTradeContractTemplate(townName, day, slot + 1, risk.label, hotGoodKey) || CONTRACT_TRADE_TEMPLATES[0]
  const fallbackFlavor = toContractFlavor(clock.phase, fallbackTemplate)
  return {
    kind: asText(fallbackTemplate.kind, 'contract_supply', 40),
    type: 'trade_n',
    reward: clamp(Number(fallbackTemplate.rewardBase || 2) + riskBonus, CONTRACT_REWARD_MIN, CONTRACT_REWARD_MAX),
    objective: { kind: 'trade_n', n: 1, ...(marketName ? { market: marketName } : {}) },
    progress: { done: 0 },
    title: asText(fallbackTemplate.title, 'CONTRACT: Quartermaster Call', 120),
    desc: `${fallbackFlavor} Secure one lot for ${townName}.`,
    risk,
    hotGood: toMarketGoodLabel(hotGoodKey)
  }
}

/**
 * @param {any} memory
 * @param {{operationId: string, idPrefix: string, tickIdx: number, at: number, towns: {townName: string}[]}} input
 */
function emitNightCaravanTrouble(memory, input) {
  const clock = ensureWorldClock(memory.world)
  const threat = ensureWorldThreat(memory.world)
  const towns = (Array.isArray(input?.towns) ? input.towns : [])
    .map((entry) => asText(entry?.townName, '', 80))
    .filter(Boolean)
    .map((townName) => ({
      townName,
      level: clamp(Math.trunc(Number(threat.byTown[townName] || 0)), 0, 100)
    }))
    .sort((a, b) => {
      const diff = b.level - a.level
      if (diff !== 0) return diff
      return a.townName.localeCompare(b.townName)
    })

  for (const row of towns) {
    if (!shouldEmitCaravanTrouble(row.townName, row.level, clock.day, clock.season)) continue
    const risk = getRouteRisk(row.townName, memory.world)
    const landmarkIdx = stableHashNumber(`${row.townName}:${clock.day}:${clock.season}:landmark`) % NIGHT_TROUBLE_LANDMARKS.length
    const landmark = NIGHT_TROUBLE_LANDMARKS[landmarkIdx] || 'outer road'
    const message = `[${row.townName}] Night report: caravan trouble near the ${landmark}. Risk ${risk.label}; keep to lit routes.`
    const townKey = row.townName.toLowerCase()
    appendChronicle(memory, {
      id: `${input.idPrefix}:chronicle:night_warning:${input.tickIdx}:${townKey}`,
      type: 'trade',
      msg: message,
      at: input.at,
      town: row.townName,
      meta: {
        day: clock.day,
        level: row.level,
        risk: risk.label
      }
    })
    appendNews(memory, {
      id: `${input.idPrefix}:news:night_warning:${input.tickIdx}:${townKey}`,
      topic: 'trade',
      msg: message,
      at: input.at,
      town: row.townName,
      meta: {
        day: clock.day,
        level: row.level,
        risk: risk.label
      }
    })
    return row.townName
  }
  return ''
}

/**
 * @param {any} memory
 * @param {{operationId: string, idPrefix: string, day: number, at: number}} input
 */
function generateDailyContracts(memory, input) {
  const world = memory.world
  const quests = ensureWorldQuests(world)
  const towns = deriveTownsFromMarkers(world?.markers || [])
  const clock = ensureWorldClock(world)
  const created = []

  for (const town of towns) {
    const townName = town.townName
    const existingToday = quests.filter((quest) => {
      if (!isContractQuest(quest)) return false
      if (!sameText(quest.town, townName, 80)) return false
      const dayValue = Number(quest.meta?.contract_day || 0)
      return dayValue === input.day
    })
    const desired = 1 + (stableHashNumber(`${townName}:${input.day}:${clock.season}`) % CONTRACT_MAX_PER_TOWN_PER_DAY)
    const missing = Math.max(0, Math.min(CONTRACT_MAX_PER_TOWN_PER_DAY, desired) - existingToday.length)
    if (missing <= 0) continue

    for (let slot = 0; slot < missing; slot += 1) {
      const draft = buildContractDraft(world, townName, towns, input.day, slot)
      const at = Number(input.at || Date.now())
      const questId = createQuestId(quests, `${input.operationId}:contract:${townName}:${input.day}:${slot}`, townName, draft.type, at + slot)
      const quest = {
        id: questId,
        type: draft.type,
        state: 'offered',
        town: townName,
        offered_at: new Date(at).toISOString(),
        objective: draft.objective,
        progress: draft.progress,
        reward: draft.reward,
        title: draft.title,
        desc: asText(`${draft.desc} Risk: ${draft.risk.label}.`, 'Contract posted.', 120),
        meta: {
          contract: true,
          kind: draft.kind,
          contract_day: input.day,
          risk: draft.risk.label,
          risk_note: draft.risk.reason,
          pulse_hot: draft.hotGood,
          season: clock.season
        }
      }
      quests.push(quest)
      created.push(quest)
      const message = `[${townName}] CONTRACT POSTED: ${quest.title} (reward ${quest.reward} emeralds).`
      appendChronicle(memory, {
        id: `${input.idPrefix}:chronicle:contract_offer:${quest.id.toLowerCase()}`,
        type: 'contract',
        msg: message,
        at,
        town: townName,
        meta: {
          quest_id: quest.id,
          kind: draft.kind,
          risk: draft.risk.label
        }
      })
      appendNews(memory, {
        id: `${input.idPrefix}:news:contract_offer:${quest.id.toLowerCase()}`,
        topic: 'trade',
        msg: message,
        at,
        town: townName,
        meta: {
          quest_id: quest.id,
          kind: draft.kind,
          risk: draft.risk.label
        }
      })
    }
  }
  return created
}

/**
 * @param {any} world
 */
function deriveTownNamesForEvents(world) {
  const names = new Map()
  const towns = deriveTownsFromMarkers(world?.markers || [])
  for (const town of towns) {
    const safeName = asText(town?.townName, '', 80)
    if (!safeName) continue
    names.set(safeName.toLowerCase(), safeName)
  }
  const threat = normalizeWorldThreat(world?.threat)
  for (const townName of Object.keys(threat.byTown || {})) {
    const safeName = asText(townName, '', 80)
    if (!safeName) continue
    const key = safeName.toLowerCase()
    if (!names.has(key)) names.set(key, safeName)
  }
  const moods = normalizeWorldMoods(world?.moods)
  for (const townName of Object.keys(moods.byTown || {})) {
    const safeName = asText(townName, '', 80)
    if (!safeName) continue
    const key = safeName.toLowerCase()
    if (!names.has(key)) names.set(key, safeName)
  }
  return Array.from(names.values()).sort((a, b) => a.localeCompare(b))
}

/**
 * @param {any} world
 */
function pickAutoEventTown(world) {
  const townNames = deriveTownNamesForEvents(world)
  if (townNames.length === 0) return '-'
  const threat = normalizeWorldThreat(world?.threat)
  const rows = townNames.map((townName) => ({
    townName,
    threat: Number(threat.byTown[townName] || 0)
  }))
  rows.sort((a, b) => {
    const diff = b.threat - a.threat
    if (diff !== 0) return diff
    return a.townName.localeCompare(b.townName)
  })
  return rows[0]?.townName || townNames[0]
}

/**
 * @param {number} seed
 * @param {number} index
 */
function drawEventType(seed, index) {
  const safeSeed = Number.isInteger(seed) ? seed : 1337
  const safeIndex = Number.isInteger(index) && index >= 0 ? index : 0
  const raw = Math.imul((safeSeed ^ (safeIndex + 1)) >>> 0, 1664525) + 1013904223
  const idx = (raw >>> 0) % EVENT_DECK.length
  return EVENT_DECK[idx]
}

/**
 * @param {any[]} activeEvents
 * @param {string} idPrefix
 * @param {string} townName
 * @param {string} type
 * @param {number} day
 * @param {number} index
 */
function createWorldEventId(activeEvents, idPrefix, townName, type, day, index) {
  const used = new Set((activeEvents || [])
    .map(entry => asText(entry?.id, '', 200).toLowerCase())
    .filter(Boolean))
  const base = asText(
    `e_${shortStableHash(`${idPrefix}:${townName}:${type}:${day}:${index}`)}`,
    `e_${shortStableHash(`${townName}:${type}:${day}:${index}`)}`,
    200
  )
  if (!used.has(base.toLowerCase())) return base
  let suffix = 2
  while (suffix < 10000) {
    const candidate = asText(`${base}-${suffix}`, base, 200)
    if (!used.has(candidate.toLowerCase())) return candidate
    suffix += 1
  }
  return asText(`${base}-${shortStableHash(`${base}:fallback`)}`, base, 200)
}

/**
 * @param {Record<string, number>} mods
 */
function summarizeEventMods(mods) {
  const normalized = normalizeWorldEventMods(mods)
  const keys = Object.keys(normalized).sort((a, b) => a.localeCompare(b))
  if (keys.length === 0) return '-'
  return keys.map((key) => {
    const value = Number(normalized[key] || 0)
    const sign = value > 0 ? '+' : ''
    return `${key}=${sign}${value}`
  }).join('|')
}

/**
 * @param {any} event
 * @param {{templateKey?: string, templateKeys?: string[]}} autoRumorConfig
 */
function pickAutoRumorTemplateKey(event, autoRumorConfig) {
  const list = []
  const primary = asText(autoRumorConfig?.templateKey, '', 80)
  if (primary) list.push(primary)
  for (const key of (Array.isArray(autoRumorConfig?.templateKeys) ? autoRumorConfig.templateKeys : [])) {
    const safe = asText(key, '', 80)
    if (!safe) continue
    if (list.some(item => sameText(item, safe, 80))) continue
    list.push(safe)
  }
  if (list.length === 0) return ''
  const safeEvent = normalizeWorldEvent(event)
  if (!safeEvent) return list[0]
  const idx = stableHashNumber(`${safeEvent.id}:${safeEvent.town}:${safeEvent.starts_day}:auto_rumor`) % list.length
  return list[idx] || list[0]
}

/**
 * @param {any} memory
 * @param {{
 *   operationId: string,
 *   idPrefix: string,
 *   at: number,
 *   requestedTownName?: string | null
 * }} input
 */
function drawAndApplyWorldEvent(memory, input) {
  const clock = ensureWorldClock(memory.world)
  const events = ensureWorldEvents(memory.world)
  const moods = ensureWorldMoods(memory.world)
  // Keep active deck bounded to currently relevant windows on each draw.
  events.active = events.active
    .map(normalizeWorldEvent)
    .filter(Boolean)
    .filter(event => event.ends_day >= clock.day)
  moods.byTown = normalizeWorldMoods(moods).byTown

  let townName = asText(input?.requestedTownName, '', 80)
  if (townName) {
    const candidates = deriveTownNamesForEvents(memory.world)
    const resolved = candidates.find(candidate => sameText(candidate, townName, 80))
    if (!resolved) {
      throw new AppError({
        code: 'UNKNOWN_TOWN',
        message: `Unknown town for event draw: ${townName}`,
        recoverable: true
      })
    }
    townName = resolved
  } else {
    townName = pickAutoEventTown(memory.world)
  }

  const drawIndex = events.index
  const type = drawEventType(events.seed, drawIndex)
  const config = EVENT_TYPE_CONFIG[type] || EVENT_TYPE_CONFIG.festival
  const mods = normalizeWorldEventMods(config.mods)
  const day = clock.day
  const eventId = createWorldEventId(
    events.active,
    asText(input?.idPrefix, '', 200) || asText(input?.operationId, '', 200),
    townName,
    type,
    day,
    drawIndex
  )
  const event = {
    id: eventId,
    type,
    town: townName,
    starts_day: day,
    ends_day: day,
    mods
  }
  events.index = drawIndex + 1
  events.active.push(event)

  const at = Number.isFinite(Number(input?.at)) ? Number(input.at) : Date.now()
  const message = `[${townName}] EVENT: ${config.title}`
  appendChronicle(memory, {
    id: `${input.idPrefix}:chronicle:event_draw:${eventId.toLowerCase()}`,
    type: 'event',
    msg: message,
    at,
    town: townName,
    meta: {
      event_id: event.id,
      event_type: event.type,
      starts_day: event.starts_day,
      ends_day: event.ends_day,
      effects: summarizeEventMods(event.mods)
    }
  })
  appendNews(memory, {
    id: `${input.idPrefix}:news:event_draw:${eventId.toLowerCase()}`,
    topic: 'world',
    msg: message,
    at,
    town: townName,
    meta: {
      event_id: event.id,
      event_type: event.type,
      effects: summarizeEventMods(event.mods)
    }
  })
  applyTownMoodDelta(memory, {
    townName,
    delta: {
      fear: Number(mods.fear || 0),
      unrest: Number(mods.unrest || 0),
      prosperity: Number(mods.prosperity || 0)
    },
    at,
    idPrefix: `${input.idPrefix}:event_mood:${eventId.toLowerCase()}`,
    reason: `event:${event.type}`
  })
  const pulseAfterDraw = getMarketPulse(townName, memory.world)
  const pulseMessage = `[${townName}] MARKET PULSE SHIFT: hot ${asText(pulseAfterDraw.hot[0]?.good, 'Bread', 40)} / cold ${asText(pulseAfterDraw.cold[0]?.good, 'Wool', 40)}.`
  appendChronicle(memory, {
    id: `${input.idPrefix}:chronicle:pulse_shift:${eventId.toLowerCase()}`,
    type: 'trade',
    msg: pulseMessage,
    at,
    town: townName,
    meta: {
      event_id: event.id,
      event_type: event.type
    }
  })
  appendNews(memory, {
    id: `${input.idPrefix}:news:pulse_shift:${eventId.toLowerCase()}`,
    topic: 'trade',
    msg: pulseMessage,
    at,
    town: townName,
    meta: {
      event_id: event.id,
      event_type: event.type
    }
  })

  let spawnedRumor = null
  const autoRumorConfig = EVENT_TO_AUTO_RUMOR[event.type]
  if (autoRumorConfig) {
    const templateKey = pickAutoRumorTemplateKey(event, autoRumorConfig)
    if (templateKey) {
      spawnedRumor = spawnWorldRumor(memory, {
        townName,
        kind: autoRumorConfig.kind,
        severity: autoRumorConfig.severity,
        templateKey,
        expiresInDays: autoRumorConfig.expiresInDays,
        at,
        idPrefix: `${input.idPrefix}:event_rumor:${eventId.toLowerCase()}`,
        spawnedByEventId: event.id
      })
    }
  }
  if (spawnedRumor) {
    const leadMessage = `[${townName}] LEAD SURFACED: ${spawnedRumor.text}`
    appendChronicle(memory, {
      id: `${input.idPrefix}:chronicle:lead_surface:${eventId.toLowerCase()}`,
      type: 'rumor',
      msg: leadMessage,
      at,
      town: townName,
      meta: {
        event_id: event.id,
        rumor_id: spawnedRumor.id
      }
    })
    appendNews(memory, {
      id: `${input.idPrefix}:news:lead_surface:${eventId.toLowerCase()}`,
      topic: 'rumor',
      msg: leadMessage,
      at,
      town: townName,
      meta: {
        event_id: event.id,
        rumor_id: spawnedRumor.id
      }
    })
  }
  return event
}

/**
 * @param {Record<string, any>} factions
 * @param {string} townName
 */
function findStoryFactionByTown(factions, townName) {
  const target = asText(townName, '', 80).toLowerCase()
  if (!target) return null
  for (const factionName of STORY_FACTION_NAMES) {
    const faction = factions[factionName]
    if (!faction || typeof faction !== 'object' || Array.isArray(faction)) continue
    const towns = normalizeStoryTownNames(faction.towns)
    if (towns.some(town => town.toLowerCase() === target)) {
      return {
        ...faction,
        name: factionName,
        towns
      }
    }
  }
  return null
}

/**
 * @param {unknown} metaInput
 */
function normalizeFeedMeta(metaInput) {
  if (!metaInput || typeof metaInput !== 'object' || Array.isArray(metaInput)) return null
  const meta = {}
  for (const [keyRaw, value] of Object.entries(metaInput)) {
    const key = asText(keyRaw, '', 80)
    if (!key) continue
    if (typeof value === 'string') {
      const text = asText(value, '', 160)
      if (text) meta[key] = text
      continue
    }
    if (typeof value === 'number') {
      if (Number.isFinite(value)) meta[key] = value
      continue
    }
    if (typeof value === 'boolean' || value === null) {
      meta[key] = value
    }
  }
  return Object.keys(meta).length > 0 ? meta : null
}

/**
 * @param {unknown} entry
 */
function normalizeChronicleEntry(entry) {
  if (!entry || typeof entry !== 'object' || Array.isArray(entry)) return null
  const id = asText(entry.id, '', 200)
  const type = asText(entry.type, '', 40).toLowerCase()
  const msg = asText(entry.msg, '', 240)
  const at = Number(entry.at)
  const town = asText(entry.town, '', 80)
  const meta = normalizeFeedMeta(entry.meta)
  if (!id || !type || !msg) return null
  if (!Number.isFinite(at) || at < 0) return null
  const next = { id, type, msg, at }
  if (town) next.town = town
  if (meta) next.meta = meta
  return next
}

/**
 * @param {unknown} entry
 */
function normalizeNewsEntry(entry) {
  if (!entry || typeof entry !== 'object' || Array.isArray(entry)) return null
  const id = asText(entry.id, '', 200)
  const topic = asText(entry.topic, '', 40).toLowerCase()
  const msg = asText(entry.msg, '', 240)
  const at = Number(entry.at)
  const town = asText(entry.town, '', 80)
  const meta = normalizeFeedMeta(entry.meta)
  if (!id || !topic || !msg) return null
  if (!Number.isFinite(at) || at < 0) return null
  const next = { id, topic, msg, at }
  if (town) next.town = town
  if (meta) next.meta = meta
  return next
}

/**
 * @param {unknown} chronicleInput
 */
function normalizeWorldChronicle(chronicleInput) {
  return (Array.isArray(chronicleInput) ? chronicleInput : [])
    .map(normalizeChronicleEntry)
    .filter(Boolean)
}

/**
 * @param {unknown} newsInput
 */
function normalizeWorldNews(newsInput) {
  return (Array.isArray(newsInput) ? newsInput : [])
    .map(normalizeNewsEntry)
    .filter(Boolean)
}

/**
 * @param {unknown} value
 */
function normalizeIsoDate(value) {
  const text = asText(value, '', 80)
  if (!text) return ''
  const atMs = Date.parse(text)
  if (!Number.isFinite(atMs)) return ''
  return new Date(atMs).toISOString()
}

/**
 * @param {'trade_n' | 'visit_town' | 'rumor_task'} type
 * @param {unknown} objectiveInput
 * @param {unknown} progressInput
 */
function normalizeQuestObjectiveProgress(type, objectiveInput, progressInput) {
  const objective = (objectiveInput && typeof objectiveInput === 'object' && !Array.isArray(objectiveInput))
    ? objectiveInput
    : null
  const progress = (progressInput && typeof progressInput === 'object' && !Array.isArray(progressInput))
    ? progressInput
    : null
  if (!objective || !progress) return null

  if (type === 'trade_n') {
    const kind = asText(objective.kind, '', 20).toLowerCase()
    const n = Number(objective.n)
    const market = asText(objective.market, '', 80)
    const done = Number(progress.done)
    if (kind !== 'trade_n') return null
    if (!Number.isInteger(n) || n < 1) return null
    if (!Number.isInteger(done) || done < 0) return null
    const nextObjective = { kind: 'trade_n', n }
    if (market) nextObjective.market = market
    return {
      objective: nextObjective,
      progress: { done }
    }
  }

  if (type === 'visit_town') {
    const kind = asText(objective.kind, '', 20).toLowerCase()
    const town = asText(objective.town, '', 80)
    if (kind !== 'visit_town' || !town) return null
    if (typeof progress.visited !== 'boolean') return null
    return {
      objective: { kind: 'visit_town', town },
      progress: { visited: progress.visited }
    }
  }

  if (type === 'rumor_task') {
    const kind = asText(objective.kind, '', 20).toLowerCase()
    const rumorId = asText(objective.rumor_id, '', 200)
    const rumorTask = asText(objective.rumor_task, '', 20).toLowerCase()
    if (kind !== 'rumor_task') return null
    if (!rumorId || !RUMOR_TASK_KINDS.has(rumorTask)) return null
    const normalizedObjective = {
      kind: 'rumor_task',
      rumor_id: rumorId,
      rumor_task: rumorTask
    }
    if (rumorTask === 'rumor_trade') {
      const n = Number(objective.n)
      const market = asText(objective.market, '', 80)
      const done = Number(progress.done)
      if (!Number.isInteger(n) || n < 1) return null
      if (!Number.isInteger(done) || done < 0) return null
      normalizedObjective.n = n
      if (market) normalizedObjective.market = market
      return {
        objective: normalizedObjective,
        progress: { done }
      }
    }
    if (rumorTask === 'rumor_visit' || rumorTask === 'rumor_choice') {
      const town = asText(objective.town, '', 80)
      if (!town) return null
      if (typeof progress.visited !== 'boolean') return null
      normalizedObjective.town = town
      return {
        objective: normalizedObjective,
        progress: { visited: progress.visited }
      }
    }
    return null
  }

  return null
}

/**
 * @param {unknown} entry
 */
function normalizeQuest(entry) {
  if (!entry || typeof entry !== 'object' || Array.isArray(entry)) return null
  const id = asText(entry.id, '', 200)
  const type = asText(entry.type, '', 20).toLowerCase()
  const state = asText(entry.state, '', 20).toLowerCase()
  const origin = asText(entry.origin, '', 40).toLowerCase()
  const town = asText(entry.town, '', 80)
  const townId = asText(entry.townId, '', 80)
  const npcKey = asText(entry.npcKey, '', 80)
  const supportsMajorMissionId = asText(entry.supportsMajorMissionId, '', 200)
  const offeredAt = normalizeIsoDate(entry.offered_at)
  const acceptedAt = normalizeIsoDate(entry.accepted_at)
  const owner = asText(entry.owner, '', 80)
  const reward = Number(entry.reward)
  const title = asText(entry.title, '', 120)
  const desc = asText(entry.desc, '', 120)
  const meta = normalizeFeedMeta(entry.meta)
  const rumorIdRaw = asText(entry.rumor_id, '', 200)

  if (!id) return null
  if (!QUEST_TYPES.has(type)) return null
  if (!QUEST_STATES.has(state)) return null
  if (!offeredAt) return null
  if (!Number.isInteger(reward) || reward < 0) return null
  if (!title || !desc) return null

  const objectiveProgress = normalizeQuestObjectiveProgress(type, entry.objective, entry.progress)
  if (!objectiveProgress) return null

  const quest = {
    id,
    type,
    state,
    offered_at: offeredAt,
    objective: objectiveProgress.objective,
    progress: objectiveProgress.progress,
    reward,
    title,
    desc
  }
  const objectiveRumorId = asText(objectiveProgress.objective?.rumor_id, '', 200)
  const rumorId = rumorIdRaw || objectiveRumorId
  if (type === 'rumor_task' && !rumorId) return null
  if (rumorId) quest.rumor_id = rumorId
  if (origin) quest.origin = origin
  if (town) quest.town = town
  if (townId) quest.townId = townId
  if (npcKey) quest.npcKey = npcKey
  if (supportsMajorMissionId) quest.supportsMajorMissionId = supportsMajorMissionId
  if (acceptedAt) quest.accepted_at = acceptedAt
  if (owner) quest.owner = owner
  if (meta) quest.meta = meta
  return quest
}

/**
 * @param {unknown} questsInput
 */
function normalizeWorldQuests(questsInput) {
  return (Array.isArray(questsInput) ? questsInput : [])
    .map(normalizeQuest)
    .filter(Boolean)
}

/**
 * @param {any} quest
 */
function isTownsfolkQuest(quest) {
  const normalized = normalizeQuest(quest)
  if (!normalized) return false
  return asText(normalized.origin, '', 40).toLowerCase() === 'townsfolk'
}

/**
 * @param {any} quest
 */
function asQuestOfferedAtMs(quest) {
  const offeredAt = normalizeIsoDate(quest?.offered_at)
  const atMs = Date.parse(offeredAt)
  if (Number.isFinite(atMs)) return atMs
  return 0
}

/**
 * @param {any} quest
 */
function isQuestActiveForHistory(quest) {
  const state = asText(quest?.state, '', 20).toLowerCase()
  return state === 'accepted' || state === 'in_progress'
}

/**
 * @param {any[]} quests
 */
function boundTownsfolkQuestHistory(quests) {
  const normalized = (Array.isArray(quests) ? quests : [])
    .map(normalizeQuest)
    .filter(Boolean)
  const byTown = new Map()
  for (const quest of normalized) {
    if (!isTownsfolkQuest(quest)) continue
    const townKey = asText(quest.townId || quest.town, '', 80).toLowerCase()
    if (!townKey) continue
    if (!byTown.has(townKey)) byTown.set(townKey, [])
    byTown.get(townKey).push(quest)
  }
  const dropIds = new Set()
  for (const questsForTown of byTown.values()) {
    if (questsForTown.length <= MAX_TOWNSFOLK_QUESTS_PER_TOWN) continue
    const active = questsForTown
      .filter(isQuestActiveForHistory)
      .sort((left, right) => asQuestOfferedAtMs(right) - asQuestOfferedAtMs(left) || left.id.localeCompare(right.id))
    const inactive = questsForTown
      .filter(quest => !isQuestActiveForHistory(quest))
      .sort((left, right) => asQuestOfferedAtMs(right) - asQuestOfferedAtMs(left) || left.id.localeCompare(right.id))
    const keep = new Set()
    for (const quest of [...active, ...inactive]) {
      if (keep.size >= MAX_TOWNSFOLK_QUESTS_PER_TOWN) break
      keep.add(quest.id.toLowerCase())
    }
    for (const quest of questsForTown) {
      if (!keep.has(quest.id.toLowerCase())) dropIds.add(quest.id.toLowerCase())
    }
  }
  if (dropIds.size === 0) return normalized
  return normalized.filter(quest => !dropIds.has(quest.id.toLowerCase()))
}

/**
 * @param {any} world
 */
function ensureWorldQuests(world) {
  world.quests = boundTownsfolkQuestHistory(normalizeWorldQuests(world.quests))
  return world.quests
}

/**
 * @param {any[]} quests
 * @param {string} questId
 */
function findQuestById(quests, questId) {
  const target = asText(questId, '', 200).toLowerCase()
  if (!target) return null
  for (const entry of quests || []) {
    const quest = normalizeQuest(entry)
    if (!quest) continue
    if (quest.id.toLowerCase() === target) return quest
  }
  return null
}

/**
 * @param {string} text
 */
function shortStableHash(text) {
  return stableHashNumber(text).toString(36)
}

/**
 * @param {string} text
 */
function stableHashNumber(text) {
  let hash = 0
  for (let i = 0; i < text.length; i += 1) {
    hash = ((hash << 5) - hash) + text.charCodeAt(i)
    hash |= 0
  }
  return Math.abs(hash)
}

/**
 * @param {any[]} quests
 * @param {string} operationId
 * @param {string} townName
 * @param {'trade_n' | 'visit_town'} type
 * @param {number} atMs
 */
function createQuestId(quests, operationId, townName, type, atMs) {
  const hashBase = shortStableHash(`${operationId}:${townName}:${type}:${atMs}`)
  const tsPart = Math.max(0, Math.floor(Number(atMs) || 0)).toString(36)
  const base = asText(`q_${hashBase}_${tsPart}`, `q_${hashBase}`, 200)
  const used = new Set((quests || [])
    .map(entry => asText(entry?.id, '', 200).toLowerCase())
    .filter(Boolean))
  if (!used.has(base.toLowerCase())) return base

  let suffix = 2
  while (suffix < 10000) {
    const candidate = asText(`${base}-${suffix}`, base, 200)
    if (!used.has(candidate.toLowerCase())) return candidate
    suffix += 1
  }
  return asText(`${base}-${shortStableHash(`${base}:fallback`)}`, base, 200)
}

/**
 * @param {unknown} npcInput
 */
function normalizeNpcKey(npcInput) {
  const name = asText(npcInput, '', 80).toLowerCase()
  if (!name) return ''
  const key = name
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
  return asText(key, 'townsfolk', 80)
}

/**
 * @param {number} day
 * @param {number} seed
 * @param {string} townName
 * @param {string} npcKey
 */
function deterministicQuestIso(day, seed, townName, npcKey) {
  const safeDay = Math.max(1, Math.trunc(Number(day) || 1))
  const offsetMs = stableHashNumber(`${seed}:${townName}:${npcKey}:${safeDay}:quest_iso`) % 86400000
  const baseEpochMs = Date.parse('2026-01-01T00:00:00.000Z')
  return new Date(baseEpochMs + ((safeDay - 1) * 86400000) + offsetMs).toISOString()
}

/**
 * @param {number} seed
 * @param {string} townName
 * @param {string} npcKey
 * @param {number} day
 */
function createTownsfolkQuestId(seed, townName, npcKey, day) {
  const hashBase = shortStableHash(`${seed}:${townName}:${npcKey}:${day}:townsfolk`)
  return asText(`sq_${hashBase}_${Math.max(0, day).toString(36)}`, `sq_${hashBase}`, 200)
}

/**
 * @param {any} world
 * @param {string} townName
 * @param {string} npcName
 */
function buildTownsfolkQuestDraft(world, townName, npcName) {
  const clock = normalizeWorldClock(world?.clock)
  const day = Math.max(1, Number(clock.day || 1))
  const seed = deriveNetherSeed(world)
  const npcKey = normalizeNpcKey(npcName)
  const questId = createTownsfolkQuestId(seed, townName, npcKey, day)
  const towns = deriveTownsFromMarkers(world?.markers || [])
  const mode = stableHashNumber(`${seed}:${townName}:${npcKey}:${day}:mode`) % 2
  const reward = 2 + (stableHashNumber(`${seed}:${townName}:${npcKey}:${day}:reward`) % 4)
  const offeredAt = deterministicQuestIso(day, seed, townName, npcKey)
  const offeredLabel = asText(npcName, npcKey, 80)
  const missionView = getTownMajorMissionView(world, townName)
  const activeMission = normalizeMajorMission(missionView?.activeMission)
  const supportsMajorMissionId = activeMission ? activeMission.id : ''

  if (mode === 0 || towns.length < 2) {
    const n = 1 + (stableHashNumber(`${seed}:${townName}:${npcKey}:${day}:trade_n`) % 2)
    const market = normalizeWorldMarkets(world?.markets)
      .filter(item => {
        const markerTown = findTownNameForMarker(world?.markers || [], asText(item?.marker, '', 80))
        return sameText(markerTown, townName, 80)
      })
      .sort((a, b) => a.name.localeCompare(b.name))[0]?.name || ''
    const objective = { kind: 'trade_n', n }
    if (market) objective.market = market
    return {
      id: questId,
      type: 'trade_n',
      state: 'offered',
      origin: 'townsfolk',
      town: townName,
      townId: townName,
      npcKey,
      offered_at: offeredAt,
      objective,
      progress: { done: 0 },
      reward,
      title: asText(`SIDE: ${offeredLabel} needs supplies`, 'SIDE: Townsfolk Supply Ask', 120),
      desc: asText(`[${townName}] ${offeredLabel} asks for ${n} quick trade lot${n === 1 ? '' : 's'}.`, 'Townsfolk request posted.', 120),
      ...(supportsMajorMissionId ? { supportsMajorMissionId } : {}),
      meta: {
        side: true,
        townsfolk: true,
        npc: offeredLabel,
        day,
        seed
      }
    }
  }

  const targetTown = pickContractRouteTarget(towns, townName, day) || townName
  return {
    id: questId,
    type: 'visit_town',
    state: 'offered',
    origin: 'townsfolk',
    town: townName,
    townId: townName,
    npcKey,
    offered_at: offeredAt,
    objective: { kind: 'visit_town', town: targetTown },
    progress: { visited: false },
    reward,
    title: asText(`SIDE: ${offeredLabel} seeks a courier`, 'SIDE: Townsfolk Courier Ask', 120),
    desc: asText(`[${townName}] ${offeredLabel} needs word carried to ${targetTown}.`, 'Townsfolk courier request posted.', 120),
    ...(supportsMajorMissionId ? { supportsMajorMissionId } : {}),
    meta: {
      side: true,
      townsfolk: true,
      npc: offeredLabel,
      day,
      seed
    }
  }
}

/**
 * @param {unknown} value
 */
function normalizeMajorMissionPayloadValue(value) {
  if (value === null) return null
  if (typeof value === 'boolean') return value
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return null
    return Math.trunc(value)
  }
  if (typeof value === 'string') {
    const text = asText(value, '', 120)
    return text || null
  }
  return null
}

/**
 * @param {unknown} payloadInput
 * @param {number} [maxKeys]
 */
function normalizeMajorMissionPayload(payloadInput, maxKeys) {
  if (!payloadInput || typeof payloadInput !== 'object' || Array.isArray(payloadInput)) return {}
  const payload = {}
  const limit = Number.isInteger(maxKeys) && maxKeys > 0 ? maxKeys : MAJOR_MISSION_MAX_PAYLOAD_KEYS
  let used = 0
  for (const [keyRaw, valueRaw] of Object.entries(payloadInput)) {
    if (used >= limit) break
    const key = asText(keyRaw, '', 40)
    if (!key) continue
    const value = normalizeMajorMissionPayloadValue(valueRaw)
    if (value === null) continue
    payload[key] = value
    used += 1
  }
  return payload
}

/**
 * @param {unknown} valueInput
 */
function normalizeNetherModifierValue(valueInput) {
  const value = Number(valueInput)
  if (!Number.isFinite(value)) return 0
  return clamp(Math.trunc(value), -9, 9)
}

/**
 * @param {unknown} modifiersInput
 */
function normalizeNetherModifiers(modifiersInput) {
  const source = (modifiersInput && typeof modifiersInput === 'object' && !Array.isArray(modifiersInput))
    ? modifiersInput
    : {}
  return {
    longNight: normalizeNetherModifierValue(source.longNight),
    omen: normalizeNetherModifierValue(source.omen),
    scarcity: normalizeNetherModifierValue(source.scarcity),
    threat: normalizeNetherModifierValue(source.threat)
  }
}

/**
 * @param {unknown} deckStateInput
 * @param {number} fallbackSeed
 */
function normalizeNetherDeckState(deckStateInput, fallbackSeed) {
  const source = (deckStateInput && typeof deckStateInput === 'object' && !Array.isArray(deckStateInput))
    ? deckStateInput
    : {}
  const seed = Number(source.seed)
  const cursor = Number(source.cursor)
  return {
    seed: Number.isInteger(seed) ? seed : fallbackSeed,
    cursor: Number.isInteger(cursor) && cursor >= 0 ? cursor : 0
  }
}

/**
 * @param {unknown} ledgerEntryInput
 */
function normalizeNetherLedgerEntry(ledgerEntryInput) {
  if (!ledgerEntryInput || typeof ledgerEntryInput !== 'object' || Array.isArray(ledgerEntryInput)) return null
  const id = asText(ledgerEntryInput.id, '', 200)
  const day = Number(ledgerEntryInput.day)
  const typeRaw = asText(ledgerEntryInput.type, '', 40).toUpperCase()
  const type = NETHER_EVENT_TYPE_ORDER.includes(typeRaw) ? typeRaw : ''
  if (!id || !Number.isInteger(day) || day < 1 || !type) return null
  return {
    id,
    day,
    type,
    payload: normalizeMajorMissionPayload(ledgerEntryInput.payload, MAX_NETHER_EVENT_PAYLOAD_KEYS),
    applied: ledgerEntryInput.applied !== false
  }
}

/**
 * @param {unknown} ledgerInput
 */
function normalizeNetherLedger(ledgerInput) {
  const ledger = (Array.isArray(ledgerInput) ? ledgerInput : [])
    .map(normalizeNetherLedgerEntry)
    .filter(Boolean)
  if (ledger.length > MAX_NETHER_EVENT_LEDGER_ENTRIES) {
    return ledger.slice(-MAX_NETHER_EVENT_LEDGER_ENTRIES)
  }
  return ledger
}

/**
 * @param {any} world
 */
function deriveNetherSeed(world) {
  const seed = Number(world?.events?.seed)
  if (Number.isInteger(seed)) return seed
  return 1337
}

/**
 * @param {unknown} netherInput
 * @param {number} fallbackSeed
 */
function normalizeNetherState(netherInput, fallbackSeed) {
  const source = (netherInput && typeof netherInput === 'object' && !Array.isArray(netherInput))
    ? netherInput
    : {}
  const safeSeed = Number.isInteger(fallbackSeed) ? fallbackSeed : 1337
  const lastTickDay = Number(source.lastTickDay)
  const nether = {
    eventLedger: normalizeNetherLedger(source.eventLedger),
    modifiers: normalizeNetherModifiers(source.modifiers),
    deckState: normalizeNetherDeckState(source.deckState, safeSeed),
    lastTickDay: Number.isInteger(lastTickDay) && lastTickDay >= 0 ? lastTickDay : 0
  }
  if (nether.lastTickDay > 0) return nether
  let maxDay = 0
  for (const entry of nether.eventLedger) {
    if (entry.day > maxDay) maxDay = entry.day
  }
  nether.lastTickDay = maxDay
  return nether
}

/**
 * @param {any} world
 */
function ensureWorldNether(world) {
  world.nether = normalizeNetherState(world.nether, deriveNetherSeed(world))
  return world.nether
}

/**
 * @param {any} world
 */
function listKnownTownNames(world) {
  const byTown = buildTownNameIndex(world)
  return Array.from(byTown.values()).sort((a, b) => a.localeCompare(b))
}

/**
 * @param {any} nether
 * @param {number} day
 */
function drawNetherEventCount(nether, day) {
  const seed = Number(nether?.deckState?.seed || 1337)
  const cursor = Number(nether?.deckState?.cursor || 0)
  const raw = stableHashNumber(`${seed}:${day}:${cursor}:nether_count`)
  return raw % (NETHER_MAX_EVENTS_PER_DAY + 1)
}

/**
 * @param {any} nether
 * @param {number} day
 * @param {number} drawIndex
 */
function drawNetherEventType(nether, day, drawIndex) {
  const seed = Number(nether?.deckState?.seed || 1337)
  const cursor = Number(nether?.deckState?.cursor || 0)
  const idx = stableHashNumber(`${seed}:${day}:${cursor}:${drawIndex}:nether_type`) % NETHER_EVENT_TYPE_ORDER.length
  return NETHER_EVENT_TYPE_ORDER[idx] || NETHER_EVENT_TYPE_ORDER[0]
}

/**
 * @param {any[]} ledger
 * @param {string} id
 */
function hasNetherLedgerEntry(ledger, id) {
  const target = asText(id, '', 200).toLowerCase()
  if (!target) return false
  return (Array.isArray(ledger) ? ledger : [])
    .some(entry => asText(entry?.id, '', 200).toLowerCase() === target)
}

/**
 * @param {number} day
 * @param {string} type
 * @param {number} seed
 * @param {number} cursor
 */
function createNetherEventId(day, type, seed, cursor) {
  const hash = shortStableHash(`${seed}:${day}:${type}:${cursor}:nether_event`)
  return asText(`ne_${hash}_${Math.max(0, day).toString(36)}`, `ne_${hash}`, 200)
}

/**
 * @param {any} modifiers
 * @param {Record<string, number>} deltas
 */
function applyNetherModifierDeltas(modifiers, deltas) {
  const next = normalizeNetherModifiers(modifiers)
  for (const key of NETHER_MODIFIER_KEYS) {
    const delta = Number(deltas?.[key] || 0)
    if (!Number.isFinite(delta) || delta === 0) continue
    next[key] = clamp(Math.trunc(next[key] + delta), -9, 9)
  }
  return next
}

/**
 * @param {any} nether
 */
function formatNetherModifierSummary(nether) {
  const modifiers = normalizeNetherModifiers(nether?.modifiers)
  return `longNight=${modifiers.longNight} omen=${modifiers.omen} scarcity=${modifiers.scarcity} threat=${modifiers.threat}`
}

/**
 * @param {any} netherEvent
 */
function buildNetherHeadline(netherEvent) {
  const type = asText(netherEvent?.type, '', 40).toUpperCase()
  const config = NETHER_EVENT_CONFIG[type] || NETHER_EVENT_CONFIG.OMEN
  const day = Number(netherEvent?.day || 0)
  return asText(`[WORLD] NETHER ${type} day=${day}: ${config.headline}`, `[WORLD] NETHER ${type}.`, 240)
}

/**
 * @param {any} memory
 * @param {{
 *   event: any,
 *   idPrefix: string,
 *   at: number
 * }} input
 */
function appendNetherAnnouncements(memory, input) {
  const event = normalizeNetherLedgerEntry(input?.event)
  if (!event) return
  const idPrefix = asText(input?.idPrefix, '', 200)
  if (!idPrefix) return
  const at = Number.isFinite(Number(input?.at)) ? Number(input.at) : 0
  const nether = ensureWorldNether(memory.world)
  const headline = buildNetherHeadline(event)
  const modifiersText = formatNetherModifierSummary(nether)
  const message = asText(`${headline} ${modifiersText}`, headline, 240)
  const towns = listKnownTownNames(memory.world)
  for (const townName of towns) {
    appendTownCrier(memory, {
      townName,
      at,
      idPrefix: `${idPrefix}:town:${townName.toLowerCase()}`,
      type: 'nether_event',
      message
    })
  }
  appendChronicle(memory, {
    id: `${idPrefix}:chronicle:nether:${event.id.toLowerCase()}`,
    type: 'nether',
    msg: message,
    at,
    meta: {
      event_id: event.id,
      event_type: event.type,
      day: event.day,
      ...normalizeNetherModifiers(nether.modifiers)
    }
  })
  appendNews(memory, {
    id: `${idPrefix}:news:nether:${event.id.toLowerCase()}`,
    topic: 'world',
    msg: message,
    at,
    meta: {
      event_id: event.id,
      event_type: event.type,
      day: event.day,
      ...normalizeNetherModifiers(nether.modifiers)
    }
  })
}

/**
 * @param {any} memory
 * @param {{day: number, idPrefix: string, at: number}} input
 */
function tickNetherForDay(memory, input) {
  const day = Number(input?.day)
  if (!Number.isInteger(day) || day < 1) return []
  const idPrefix = asText(input?.idPrefix, '', 200)
  if (!idPrefix) return []
  const nether = ensureWorldNether(memory.world)
  if (day <= Number(nether.lastTickDay || 0)) return []

  const drawCount = drawNetherEventCount(nether, day)
  const applied = []
  for (let drawIdx = 0; drawIdx < drawCount; drawIdx += 1) {
    const seed = Number(nether.deckState.seed || 1337)
    const cursor = Number(nether.deckState.cursor || 0)
    const type = drawNetherEventType(nether, day, drawIdx)
    const eventId = createNetherEventId(day, type, seed, cursor)
    nether.deckState.cursor = cursor + 1
    if (hasNetherLedgerEntry(nether.eventLedger, eventId)) continue
    const config = NETHER_EVENT_CONFIG[type] || NETHER_EVENT_CONFIG.OMEN
    const deltas = config.deltas || {}
    nether.modifiers = applyNetherModifierDeltas(nether.modifiers, deltas)
    const entry = normalizeNetherLedgerEntry({
      id: eventId,
      day,
      type,
      payload: {
        cursor,
        ...deltas
      },
      applied: true
    })
    if (!entry) continue
    appendBounded(nether.eventLedger, entry, MAX_NETHER_EVENT_LEDGER_ENTRIES)
    appendNetherAnnouncements(memory, {
      event: entry,
      idPrefix: `${idPrefix}:event:${eventId.toLowerCase()}`,
      at: Number(input?.at || 0)
    })
    applied.push(entry)
  }
  nether.lastTickDay = day
  return applied
}

/**
 * @param {any} memory
 * @param {{nDays: number, idPrefix: string, at: number}} input
 */
function advanceNetherByDays(memory, input) {
  const nether = ensureWorldNether(memory.world)
  const nDays = clamp(Math.trunc(Number(input?.nDays || 1)), 1, 1000)
  let day = Number(nether.lastTickDay || 0)
  const applied = []
  for (let idx = 0; idx < nDays; idx += 1) {
    day += 1
    const entries = tickNetherForDay(memory, {
      day,
      at: input.at,
      idPrefix: `${input.idPrefix}:day:${day}`
    })
    for (const entry of entries) applied.push(entry)
  }
  return {
    lastTickDay: nether.lastTickDay,
    applied
  }
}

/**
 * @param {any} memory
 * @param {{targetDay: number, idPrefix: string, at: number}} input
 */
function advanceNetherToDay(memory, input) {
  const nether = ensureWorldNether(memory.world)
  const targetDay = Math.max(0, Math.trunc(Number(input?.targetDay || 0)))
  if (targetDay <= Number(nether.lastTickDay || 0)) {
    return { lastTickDay: nether.lastTickDay, applied: [] }
  }
  const applied = []
  for (let day = Number(nether.lastTickDay || 0) + 1; day <= targetDay; day += 1) {
    const entries = tickNetherForDay(memory, {
      day,
      at: input.at,
      idPrefix: `${input.idPrefix}:day:${day}`
    })
    for (const entry of entries) applied.push(entry)
  }
  return {
    lastTickDay: nether.lastTickDay,
    applied
  }
}

/**
 * @param {unknown} missionInput
 */
function normalizeMajorMission(missionInput) {
  if (!missionInput || typeof missionInput !== 'object' || Array.isArray(missionInput)) return null
  const id = asText(missionInput.id, '', 200)
  const townId = asText(missionInput.townId, '', 80)
  const templateId = asText(missionInput.templateId, '', 80).toLowerCase()
  const status = asText(missionInput.status, '', 20).toLowerCase()
  const issuedAtDay = Number(missionInput.issuedAtDay)
  const acceptedAtDayRaw = Number(missionInput.acceptedAtDay)
  const phaseRaw = missionInput.phase

  if (!id || !townId || !templateId || !MAJOR_MISSION_STATUSES.has(status)) return null
  if (!Number.isInteger(issuedAtDay) || issuedAtDay < 1) return null
  if (!Number.isInteger(acceptedAtDayRaw) || acceptedAtDayRaw < 0) return null

  let phase = null
  if (Number.isInteger(phaseRaw) && phaseRaw >= 0) {
    phase = phaseRaw
  } else {
    const phaseText = asText(phaseRaw, '', 40)
    if (phaseText) phase = phaseText
  }
  if (phase === null) return null

  return {
    id,
    townId,
    templateId,
    status,
    phase,
    issuedAtDay,
    acceptedAtDay: acceptedAtDayRaw,
    stakes: normalizeMajorMissionPayload(missionInput.stakes),
    progress: normalizeMajorMissionPayload(missionInput.progress)
  }
}

/**
 * @param {unknown} missionsInput
 */
function normalizeWorldMajorMissions(missionsInput) {
  return (Array.isArray(missionsInput) ? missionsInput : [])
    .map(normalizeMajorMission)
    .filter(Boolean)
}

/**
 * @param {any} world
 */
function ensureWorldMajorMissions(world) {
  world.majorMissions = normalizeWorldMajorMissions(world.majorMissions)
  return world.majorMissions
}

/**
 * @param {unknown} crierEntryInput
 */
function normalizeTownCrierEntry(crierEntryInput) {
  if (!crierEntryInput || typeof crierEntryInput !== 'object' || Array.isArray(crierEntryInput)) return null
  const id = asText(crierEntryInput.id, '', 200)
  const day = Number(crierEntryInput.day)
  const type = asText(crierEntryInput.type, '', 40).toLowerCase()
  const message = asText(crierEntryInput.message, '', 240)
  const missionId = asText(crierEntryInput.missionId, '', 200)
  if (!id || !Number.isInteger(day) || day < 0 || !type || !message) return null
  const entry = { id, day, type, message }
  if (missionId) entry.missionId = missionId
  return entry
}

/**
 * @param {unknown} queueInput
 */
function normalizeTownCrierQueue(queueInput) {
  const queue = (Array.isArray(queueInput) ? queueInput : [])
    .map(normalizeTownCrierEntry)
    .filter(Boolean)
  if (queue.length > TOWN_CRIER_QUEUE_MAX_ENTRIES) {
    return queue.slice(-TOWN_CRIER_QUEUE_MAX_ENTRIES)
  }
  return queue
}

/**
 * @param {unknown} townInput
 */
function normalizeTownMissionState(townInput) {
  const source = (townInput && typeof townInput === 'object' && !Array.isArray(townInput))
    ? townInput
    : {}
  const activeMajorMissionId = asText(source.activeMajorMissionId, '', 200) || null
  const cooldown = Number(source.majorMissionCooldownUntilDay)
  return {
    activeMajorMissionId,
    majorMissionCooldownUntilDay: Number.isInteger(cooldown) && cooldown >= 0 ? cooldown : 0,
    crierQueue: normalizeTownCrierQueue(source.crierQueue)
  }
}

/**
 * @param {unknown} townsInput
 */
function normalizeWorldTownMissionStates(townsInput) {
  const source = (townsInput && typeof townsInput === 'object' && !Array.isArray(townsInput))
    ? townsInput
    : {}
  const towns = {}
  for (const [townRaw, townState] of Object.entries(source)) {
    const townName = asText(townRaw, '', 80)
    if (!townName) continue
    towns[townName] = normalizeTownMissionState(townState)
  }
  return towns
}

/**
 * @param {any} world
 */
function deriveTownNamesForMissionState(world) {
  const names = new Map()
  const addTownName = (townRaw) => {
    const townName = asText(townRaw, '', 80)
    if (!townName) return
    const key = townName.toLowerCase()
    if (!names.has(key)) names.set(key, townName)
  }

  for (const townName of deriveTownNamesForEvents(world)) addTownName(townName)
  for (const townName of Object.keys(normalizeWorldTownMissionStates(world?.towns))) addTownName(townName)
  for (const mission of normalizeWorldMajorMissions(world?.majorMissions)) addTownName(mission.townId)
  for (const faction of Object.values(normalizeWorldStoryFactions(world?.factions))) {
    for (const townName of normalizeStoryTownNames(faction?.towns)) addTownName(townName)
  }
  return Array.from(names.values()).sort((a, b) => a.localeCompare(b))
}

/**
 * @param {any} world
 */
function ensureWorldTownMissionStates(world) {
  world.towns = normalizeWorldTownMissionStates(world.towns)
  for (const townName of deriveTownNamesForMissionState(world)) {
    if (!Object.prototype.hasOwnProperty.call(world.towns, townName)) {
      world.towns[townName] = normalizeTownMissionState(null)
    }
  }
  return world.towns
}

/**
 * @param {any} world
 */
function enforceMajorMissionTownExclusivity(world) {
  const missions = ensureWorldMajorMissions(world)
  const towns = ensureWorldTownMissionStates(world)
  const activeByTown = new Map()

  for (let idx = 0; idx < missions.length; idx += 1) {
    const mission = normalizeMajorMission(missions[idx])
    if (!mission) continue
    if (mission.status === 'active') {
      const key = mission.townId.toLowerCase()
      if (activeByTown.has(key)) {
        mission.status = 'briefed'
      } else {
        activeByTown.set(key, { townName: mission.townId, missionId: mission.id })
      }
    }
    missions[idx] = mission
  }

  for (const townName of Object.keys(towns)) {
    towns[townName].activeMajorMissionId = null
    towns[townName].crierQueue = normalizeTownCrierQueue(towns[townName].crierQueue)
    const cooldown = Number(towns[townName].majorMissionCooldownUntilDay)
    towns[townName].majorMissionCooldownUntilDay = Number.isInteger(cooldown) && cooldown >= 0 ? cooldown : 0
  }
  for (const { townName, missionId } of activeByTown.values()) {
    const canonicalTown = resolveTownName(world, townName) || townName
    if (!Object.prototype.hasOwnProperty.call(towns, canonicalTown)) {
      towns[canonicalTown] = normalizeTownMissionState(null)
    }
    towns[canonicalTown].activeMajorMissionId = missionId
  }
}

/**
 * @param {any[]} missions
 * @param {string} missionId
 */
function findMajorMissionById(missions, missionId) {
  const target = asText(missionId, '', 200).toLowerCase()
  if (!target) return null
  for (const entry of missions || []) {
    const mission = normalizeMajorMission(entry)
    if (!mission) continue
    if (mission.id.toLowerCase() === target) return mission
  }
  return null
}

/**
 * @param {any[]} missions
 * @param {string} townName
 */
function listMajorMissionsForTown(missions, townName) {
  return (missions || [])
    .map(normalizeMajorMission)
    .filter(Boolean)
    .filter(mission => sameText(mission.townId, townName, 80))
    .sort((left, right) => {
      const dayDiff = Number(right.issuedAtDay || 0) - Number(left.issuedAtDay || 0)
      if (dayDiff !== 0) return dayDiff
      return left.id.localeCompare(right.id)
    })
}

/**
 * @param {string} templateId
 */
function findMajorMissionTemplateById(templateId) {
  const safeTemplateId = asText(templateId, '', 80).toLowerCase()
  return MAJOR_MISSION_TEMPLATES.find(template => template.id === safeTemplateId) || null
}

/**
 * @param {any} mission
 */
function getMajorMissionPhaseNote(mission) {
  const normalized = normalizeMajorMission(mission)
  if (!normalized) return ''
  const template = findMajorMissionTemplateById(normalized.templateId)
  const phase = Number(normalized.phase)
  if (!Number.isInteger(phase) || phase < 1) return ''
  const note = template?.phaseNotes?.[phase - 1]
  return asText(note, '', 160)
}

/**
 * @param {any} mission
 */
function formatMajorMissionStakes(mission) {
  const normalized = normalizeMajorMission(mission)
  if (!normalized) return 'risk=- hot=- weak=- threat=-'
  const risk = asText(normalized.stakes?.risk, '-', 40) || '-'
  const hot = asText(normalized.stakes?.hotGood, '-', 40) || '-'
  const weak = asText(normalized.stakes?.weakGood, '-', 40) || '-'
  const threat = Number(normalized.stakes?.threat)
  const threatText = Number.isInteger(threat) ? String(threat) : '-'
  return `risk=${risk} hot=${hot} weak=${weak} threat=${threatText}`
}

/**
 * @param {any} world
 * @param {string} townName
 */
function getTownMajorMissionView(world, townName) {
  const canonicalTown = resolveTownName(world, townName)
  if (!canonicalTown) return null
  enforceMajorMissionTownExclusivity(world)
  const missions = ensureWorldMajorMissions(world)
  const towns = ensureWorldTownMissionStates(world)
  const clock = ensureWorldClock(world)
  const townState = towns[canonicalTown] || normalizeTownMissionState(null)
  const missionsByTown = listMajorMissionsForTown(missions, canonicalTown)

  let activeMission = null
  if (townState.activeMajorMissionId) {
    activeMission = findMajorMissionById(missionsByTown, townState.activeMajorMissionId)
  }
  if (!activeMission) {
    activeMission = missionsByTown.find(mission => mission.status === 'active') || null
  }
  const availableMission = missionsByTown.find(mission => (
    mission.status === 'briefed' || mission.status === 'teased'
  )) || null

  return {
    townName: canonicalTown,
    day: clock.day,
    townState,
    activeMission,
    availableMission,
    missionsByTown
  }
}

/**
 * @param {any[]} missions
 * @param {string} townName
 * @param {number} day
 * @param {string} templateId
 * @param {number} serial
 */
function createMajorMissionId(missions, townName, day, templateId, serial) {
  const hashBase = shortStableHash(`${townName}:${day}:${templateId}:${serial}:major_mission`)
  const base = asText(`mm_${hashBase}_${Math.max(0, day).toString(36)}`, `mm_${hashBase}`, 200)
  const used = new Set((missions || [])
    .map(entry => asText(entry?.id, '', 200).toLowerCase())
    .filter(Boolean))
  if (!used.has(base.toLowerCase())) return base
  let suffix = 2
  while (suffix < 10000) {
    const candidate = asText(`${base}-${suffix}`, base, 200)
    if (!used.has(candidate.toLowerCase())) return candidate
    suffix += 1
  }
  return asText(`${base}-${shortStableHash(`${base}:fallback`)}`, base, 200)
}

/**
 * @param {string} townName
 * @param {number} day
 * @param {string} season
 * @param {number} serial
 */
function pickMajorMissionTemplate(townName, day, season, serial) {
  const templates = MAJOR_MISSION_TEMPLATES.slice()
    .sort((left, right) => left.id.localeCompare(right.id))
  if (templates.length === 0) return null
  const idx = stableHashNumber(`${townName}:${day}:${season}:${serial}:major_template`) % templates.length
  return templates[idx] || templates[0]
}

/**
 * @param {any} world
 * @param {string} townName
 */
function buildMajorMissionStakes(world, townName) {
  const pulse = getMarketPulse(townName, world)
  const risk = getRouteRisk(townName, world)
  const threat = clamp(
    Math.trunc(Number(normalizeWorldThreat(world?.threat).byTown[townName] || 0)),
    0,
    100
  )
  return normalizeMajorMissionPayload({
    risk: risk.label,
    hotGood: asText(pulse.hot[0]?.good, 'Bread', 40),
    weakGood: asText(pulse.cold[0]?.good, 'Wool', 40),
    threat
  })
}

/**
 * @param {any} world
 * @param {string} townName
 */
function createMajorMissionDraft(world, townName) {
  const missions = ensureWorldMajorMissions(world)
  const clock = ensureWorldClock(world)
  const serial = listMajorMissionsForTown(missions, townName).length + 1
  const template = pickMajorMissionTemplate(townName, clock.day, clock.season, serial)
    || MAJOR_MISSION_TEMPLATES[0]
  const id = createMajorMissionId(missions, townName, clock.day, template.id, serial)
  return normalizeMajorMission({
    id,
    townId: townName,
    templateId: template.id,
    status: 'briefed',
    phase: 0,
    issuedAtDay: clock.day,
    acceptedAtDay: 0,
    stakes: buildMajorMissionStakes(world, townName),
    progress: { advances: 0 }
  })
}

/**
 * @param {any} memory
 * @param {{
 *   townName: string,
 *   at: number,
 *   idPrefix: string,
 *   type: string,
 *   message: string,
 *   missionId?: string
 * }} input
 */
function appendTownCrier(memory, input) {
  const townName = asText(input?.townName, '', 80)
  if (!townName) return null
  const towns = ensureWorldTownMissionStates(memory.world)
  if (!Object.prototype.hasOwnProperty.call(towns, townName)) {
    towns[townName] = normalizeTownMissionState(null)
  }
  const queue = normalizeTownCrierQueue(towns[townName].crierQueue)
  const clock = ensureWorldClock(memory.world)
  const day = Number.isInteger(Number(clock.day)) ? Number(clock.day) : 0
  const type = asText(input?.type, '', 40).toLowerCase()
  const message = asText(input?.message, '', 240)
  const missionId = asText(input?.missionId, '', 200)
  const idPrefix = asText(input?.idPrefix, '', 200)
  const crierId = asText(`${idPrefix}:crier:${type}:${(missionId || townName).toLowerCase()}`, '', 200)
  const entry = normalizeTownCrierEntry({
    id: crierId,
    day,
    type,
    message,
    missionId: missionId || undefined
  })
  if (!entry) return null
  queue.push(entry)
  if (queue.length > TOWN_CRIER_QUEUE_MAX_ENTRIES) {
    queue.splice(0, queue.length - TOWN_CRIER_QUEUE_MAX_ENTRIES)
  }
  towns[townName].crierQueue = queue
  return entry
}

/**
 * @param {any} memory
 * @param {{
 *   townName: string,
 *   mission: any,
 *   at: number,
 *   idPrefix: string,
 *   crierType: string,
 *   message: string,
 *   status: string
 * }} input
 */
function appendMajorMissionAnnouncements(memory, input) {
  const mission = normalizeMajorMission(input?.mission)
  if (!mission) return
  const townName = asText(input?.townName, '', 80)
  const message = asText(input?.message, '', 240)
  const idPrefix = asText(input?.idPrefix, '', 200)
  const crierType = asText(input?.crierType, '', 40).toLowerCase()
  const status = asText(input?.status, '', 20).toLowerCase()
  const at = Number.isFinite(Number(input?.at)) ? Number(input.at) : Date.now()
  if (!townName || !message || !idPrefix || !crierType) return

  appendTownCrier(memory, {
    townName,
    at,
    idPrefix,
    type: crierType,
    message,
    missionId: mission.id
  })

  const meta = {
    mission_id: mission.id,
    template_id: mission.templateId,
    status: status || mission.status,
    phase: Number.isInteger(Number(mission.phase)) ? Number(mission.phase) : 0
  }
  appendChronicle(memory, {
    id: `${idPrefix}:chronicle:major_mission:${crierType}:${mission.id.toLowerCase()}`,
    type: 'major_mission',
    msg: message,
    at,
    town: townName,
    meta
  })
  appendNews(memory, {
    id: `${idPrefix}:news:major_mission:${crierType}:${mission.id.toLowerCase()}`,
    topic: 'mission',
    msg: message,
    at,
    town: townName,
    meta
  })
}

/**
 * @param {string} townName
 * @param {any} mission
 */
function buildMajorMissionTeaserMessage(townName, mission) {
  const normalized = normalizeMajorMission(mission)
  if (!normalized) return ''
  const template = findMajorMissionTemplateById(normalized.templateId)
  const teaser = asText(template?.teaser, 'A major mission is available.', 160)
  return asText(`[${townName}] MAYOR TEASER: ${teaser}`, teaser, 240)
}

/**
 * @param {string} townName
 * @param {any} mission
 */
function buildMajorMissionBriefingMessage(townName, mission) {
  const normalized = normalizeMajorMission(mission)
  if (!normalized) return ''
  const template = findMajorMissionTemplateById(normalized.templateId)
  const title = asText(template?.title, normalized.templateId, 80)
  const briefing = asText(template?.briefing, 'Major mission briefing posted.', 160)
  return asText(
    `[${townName}] MAYOR BRIEFING: ${title}. ${briefing} Stakes: ${formatMajorMissionStakes(normalized)}`,
    briefing,
    240
  )
}

/**
 * @param {string} townName
 * @param {any} mission
 */
function buildMajorMissionAcceptanceMessage(townName, mission) {
  const normalized = normalizeMajorMission(mission)
  if (!normalized) return ''
  const template = findMajorMissionTemplateById(normalized.templateId)
  const title = asText(template?.title, normalized.templateId, 80)
  return asText(
    `[${townName}] MAYOR ACCEPTED: ${title} begins at phase ${normalized.phase}.`,
    `${title} begins.`,
    240
  )
}

/**
 * @param {string} townName
 * @param {any} mission
 */
function buildMajorMissionPhaseChangeMessage(townName, mission) {
  const normalized = normalizeMajorMission(mission)
  if (!normalized) return ''
  const note = getMajorMissionPhaseNote(normalized)
  const detail = note ? ` ${note}` : ''
  return asText(
    `[${townName}] MAJOR MISSION PHASE ${normalized.phase}.${detail}`,
    `Phase ${normalized.phase}.`,
    240
  )
}

/**
 * @param {string} townName
 * @param {any} mission
 */
function buildMajorMissionCompletionMessage(townName, mission) {
  const normalized = normalizeMajorMission(mission)
  if (!normalized) return ''
  const template = findMajorMissionTemplateById(normalized.templateId)
  const title = asText(template?.title, normalized.templateId, 80)
  return asText(`[${townName}] MAJOR MISSION WON: ${title} is complete.`, `${title} complete.`, 240)
}

/**
 * @param {string} townName
 * @param {any} mission
 * @param {string | null} reason
 */
function buildMajorMissionFailureMessage(townName, mission, reason) {
  const normalized = normalizeMajorMission(mission)
  if (!normalized) return ''
  const template = findMajorMissionTemplateById(normalized.templateId)
  const title = asText(template?.title, normalized.templateId, 80)
  const safeReason = asText(reason, '', 120)
  const reasonSuffix = safeReason ? ` Reason: ${safeReason}.` : ''
  return asText(`[${townName}] MAJOR MISSION FAILED: ${title}.${reasonSuffix}`, `${title} failed.`, 240)
}

/**
 * @param {any} quest
 */
function isQuestObjectiveSatisfied(quest) {
  const normalized = normalizeQuest(quest)
  if (!normalized) return false
  if (normalized.type === 'trade_n') {
    return Number(normalized.progress.done || 0) >= Number(normalized.objective.n || 0)
  }
  if (normalized.type === 'visit_town') {
    return normalized.progress.visited === true
  }
  if (normalized.type === 'rumor_task') {
    const rumorTask = asText(normalized.objective.rumor_task, '', 20).toLowerCase()
    if (rumorTask === 'rumor_trade') {
      return Number(normalized.progress.done || 0) >= Number(normalized.objective.n || 0)
    }
    if (rumorTask === 'rumor_visit' || rumorTask === 'rumor_choice') {
      return normalized.progress.visited === true
    }
  }
  return false
}

/**
 * @param {any} quest
 */
function summarizeQuestProgress(quest) {
  const normalized = normalizeQuest(quest)
  if (!normalized) return '-'
  if (normalized.type === 'trade_n') {
    return `${Number(normalized.progress.done || 0)}/${Number(normalized.objective.n || 0)}`
  }
  if (normalized.type === 'visit_town') {
    return normalized.progress.visited ? 'visited' : 'pending'
  }
  if (normalized.type === 'rumor_task') {
    const rumorTask = asText(normalized.objective.rumor_task, '', 20).toLowerCase()
    if (rumorTask === 'rumor_trade') {
      return `${Number(normalized.progress.done || 0)}/${Number(normalized.objective.n || 0)}`
    }
    if (rumorTask === 'rumor_visit' || rumorTask === 'rumor_choice') {
      return normalized.progress.visited ? 'visited' : 'pending'
    }
  }
  return '-'
}

/**
 * @param {any} quest
 * @param {string | null} townFallback
 * @param {number} at
 * @param {string} idPrefix
 * @param {any} memory
 */
function completeQuestAndReward(quest, townFallback, at, idPrefix, memory) {
  const normalized = normalizeQuest(quest)
  if (!normalized) {
    throw new AppError({
      code: 'INVALID_QUEST',
      message: 'Quest shape is invalid.',
      recoverable: true
    })
  }
  const ownerName = asText(normalized.owner, '', 80)
  if (!ownerName) {
    throw new AppError({
      code: 'UNKNOWN_AGENT',
      message: 'Quest owner is missing.',
      recoverable: true
    })
  }

  const clock = ensureWorldClock(memory.world)
  const town = asText(normalized.town, '', 80) || asText(townFallback, '', 80) || '-'
  const activeEvents = findActiveEventsForTown(memory.world, town, clock.day)
  const ironPactRepBonus = sumEventModifier(activeEvents, 'iron_pact_rep_bonus')
  const veilChurchRepBonus = sumEventModifier(activeEvents, 'veil_church_rep_bonus')
  const storyFactions = ensureWorldStoryFactions(memory.world)
  const townFaction = findStoryFactionByTown(storyFactions, town)
  const rumorQuestFactionBonus = (
    normalized.type === 'rumor_task'
    && townFaction
    && STORY_FACTION_NAME_SET.has(townFaction.name)
  )
    ? { [townFaction.name]: 1 }
    : {}

  const economy = ensureWorldEconomy(memory.world)
  const reward = Number(normalized.reward || 0)
  if (reward > 0) {
    const current = Number(economy.ledger[ownerName] || 0)
    economy.ledger[ownerName] = current + reward
    economy.minted_total = Number(economy.minted_total || 0) + reward
  }
  const profile = ensureAgentStoryProfile(ensureAgentProfile(memory, ownerName))
  if (
    ironPactRepBonus !== 0
    || veilChurchRepBonus !== 0
    || Object.keys(rumorQuestFactionBonus).length > 0
  ) {
    const rep = normalizeAgentRep(profile.rep)
    if (ironPactRepBonus !== 0) {
      rep.iron_pact = Number(rep.iron_pact || 0) + ironPactRepBonus
    }
    if (veilChurchRepBonus !== 0) {
      rep.veil_church = Number(rep.veil_church || 0) + veilChurchRepBonus
    }
    for (const [factionName, delta] of Object.entries(rumorQuestFactionBonus)) {
      rep[factionName] = Number(rep[factionName] || 0) + Number(delta || 0)
    }
    profile.rep = rep
    applyRepThresholdTitleAwards(memory, ownerName, at, `${idPrefix}:rep_title`, town)
  }
  if (normalized.type === 'rumor_task') {
    const currentCompleted = normalizeRumorsCompleted(profile.rumors_completed)
    profile.rumors_completed = currentCompleted + 1
    if (profile.rumors_completed >= 3) {
      grantAgentTitleIfMissing(memory, ownerName, 'Wanderer', at, `${idPrefix}:wanderer`, town)
    }
    if (normalized.rumor_id) {
      const rumors = ensureWorldRumors(memory.world)
      const rumorIdx = rumors.findIndex(entry => sameText(entry?.id, normalized.rumor_id, 200))
      if (rumorIdx >= 0) {
        const rumor = normalizeRumor(rumors[rumorIdx])
        if (rumor && !asText(rumor.resolved_by_quest_id, '', 200)) {
          rumors[rumorIdx] = {
            ...rumor,
            resolved_by_quest_id: normalized.id
          }
        }
      }
    }
  }

  normalized.state = 'completed'
  const repParts = []
  if (ironPactRepBonus > 0) repParts.push(`+${ironPactRepBonus} iron_pact rep`)
  if (veilChurchRepBonus > 0) repParts.push(`+${veilChurchRepBonus} veil_church rep`)
  for (const [factionName, delta] of Object.entries(rumorQuestFactionBonus)) {
    if (delta > 0) repParts.push(`+${delta} ${factionName} rep`)
  }
  const msg = `QUEST: ${ownerName} completed ${normalized.id} (+${reward} emeralds${repParts.length ? `, ${repParts.join(', ')}` : ''})`
  appendChronicle(memory, {
    id: `${idPrefix}:chronicle:quest_complete:${normalized.id.toLowerCase()}`,
    type: 'quest_complete',
    msg,
    at,
    town: town || undefined,
    meta: {
      quest_id: normalized.id,
      owner: ownerName,
      reward,
      iron_pact_rep_bonus: ironPactRepBonus,
      veil_church_rep_bonus: veilChurchRepBonus,
      rumor_id: normalized.rumor_id || ''
    }
  })
  appendNews(memory, {
    id: `${idPrefix}:news:quest_complete:${normalized.id.toLowerCase()}`,
    topic: 'quest',
    msg,
    at,
    town: town || undefined,
    meta: {
      quest_id: normalized.id,
      owner: ownerName,
      reward,
      iron_pact_rep_bonus: ironPactRepBonus,
      veil_church_rep_bonus: veilChurchRepBonus,
      rumor_id: normalized.rumor_id || ''
    }
  })
  applyTownMoodDelta(memory, {
    townName: town,
    delta: { prosperity: 2, fear: -1, unrest: -1 },
    at,
    idPrefix: `${idPrefix}:quest_complete:${normalized.id.toLowerCase()}`,
    reason: 'quest_complete'
  })
  return normalized
}

/**
 * @param {'trade_n' | 'visit_town'} type
 * @param {string} sourceTown
 * @param {any} objective
 */
function buildQuestFlavor(type, sourceTown, objective) {
  if (type === 'trade_n') {
    const n = Number(objective.n || 0)
    const market = asText(objective.market, '', 80)
    const title = 'Supply Run'
    const desc = market
      ? `Buy ${n} lot${n === 1 ? '' : 's'} at ${market} for town ${sourceTown}.`
      : `Buy ${n} lot${n === 1 ? '' : 's'} for town ${sourceTown}.`
    return {
      title: asText(title, 'Supply Run', 120),
      desc: asText(desc, `Buy ${n} lots.`, 120)
    }
  }
  const targetTown = asText(objective.town, sourceTown, 80) || sourceTown
  return {
    title: 'Scout the Roads',
    desc: asText(`Visit town ${targetTown} and report back to ${sourceTown}.`, 'Visit town and report back.', 120)
  }
}

/**
 * @param {any} rumor
 */
function selectRumorQuestSubtype(rumor) {
  const kind = asText(rumor?.kind, '', 20).toLowerCase()
  if (kind === 'grounded') return 'rumor_trade'
  if (kind === 'political') return 'rumor_choice'
  return 'rumor_visit'
}

/**
 * @param {any} rumor
 * @param {string} subtype
 */
function buildRumorQuestFlavor(rumor, subtype) {
  const townName = asText(rumor?.town, 'town', 80) || 'town'
  if (subtype === 'rumor_trade') {
    return {
      title: 'SIDE: Quiet Supply Run',
      desc: asText(`Help steady ${townName} by closing one urgent trade.`, 'Close one urgent trade.', 120)
    }
  }
  if (subtype === 'rumor_choice') {
    return {
      title: 'SIDE: Hear the Crowd',
      desc: asText(`Visit ${townName} and confirm which whisper is true.`, 'Visit town and hear the crowd.', 120)
    }
  }
  return {
    title: 'SIDE: Walk the Lanes',
    desc: asText(`Visit ${townName} and check the streets after dark.`, 'Visit town and check the streets.', 120)
  }
}

/**
 * @param {any} memory
 * @param {any} rumor
 * @param {{operationId: string, at: number}} input
 */
function createRumorSideQuest(memory, rumor, input) {
  const normalizedRumor = normalizeRumor(rumor)
  if (!normalizedRumor) {
    throw new AppError({
      code: 'UNKNOWN_RUMOR',
      message: 'Unknown rumor.',
      recoverable: true
    })
  }
  const clock = ensureWorldClock(memory.world)
  if (normalizedRumor.expires_day < clock.day) {
    throw new AppError({
      code: 'RUMOR_EXPIRED',
      message: 'Rumor has expired.',
      recoverable: true
    })
  }
  const quests = ensureWorldQuests(memory.world)
  const at = Number.isFinite(Number(input.at)) ? Number(input.at) : Date.now()
  const subtype = selectRumorQuestSubtype(normalizedRumor)
  const reward = clamp(Number(normalizedRumor.severity || 1) + 1, 0, 4)
  const questId = createQuestId(quests, input.operationId, normalizedRumor.town, 'rumor_task', at)
  const objective = {
    kind: 'rumor_task',
    rumor_id: normalizedRumor.id,
    rumor_task: subtype
  }
  let progress
  if (subtype === 'rumor_trade') {
    objective.n = 1
    progress = { done: 0 }
  } else {
    objective.town = normalizedRumor.town
    progress = { visited: false }
  }
  const flavor = buildRumorQuestFlavor(normalizedRumor, subtype)
  const quest = {
    id: questId,
    type: 'rumor_task',
    rumor_id: normalizedRumor.id,
    state: 'offered',
    town: normalizedRumor.town,
    offered_at: new Date(at).toISOString(),
    objective,
    progress,
    reward,
    title: flavor.title,
    desc: flavor.desc,
    meta: {
      side: true,
      rumor_id: normalizedRumor.id
    }
  }
  quests.push(quest)
  const message = `SIDE QUEST: born from rumor ${normalizedRumor.id} -> ${quest.id}`
  appendChronicle(memory, {
    id: `${input.operationId}:chronicle:rumor_quest:${quest.id.toLowerCase()}`,
    type: 'quest_offer',
    msg: message,
    at,
    town: normalizedRumor.town,
    meta: {
      quest_id: quest.id,
      quest_type: quest.type,
      rumor_id: normalizedRumor.id
    }
  })
  appendNews(memory, {
    id: `${input.operationId}:news:rumor_quest:${quest.id.toLowerCase()}`,
    topic: 'quest',
    msg: message,
    at,
    town: normalizedRumor.town,
    meta: {
      quest_id: quest.id,
      quest_type: quest.type,
      rumor_id: normalizedRumor.id
    }
  })
  return quest
}

/**
 * @param {any} memory
 * @param {any} decision
 * @param {any} option
 * @param {{operationId: string, at: number}} input
 */
function applyDecisionChoiceEffects(memory, decision, option, input) {
  const normalizedDecision = normalizeDecision(decision)
  const normalizedOption = normalizeDecisionOption(option)
  if (!normalizedDecision || !normalizedOption) return null
  const at = Number.isFinite(Number(input.at)) ? Number(input.at) : Date.now()
  const idPrefix = asText(input.operationId, '', 200)
  const effects = normalizeDecisionEffects(normalizedOption.effects) || {}
  const summary = []

  if (effects.mood) {
    applyTownMoodDelta(memory, {
      townName: normalizedDecision.town,
      delta: effects.mood,
      at,
      idPrefix: `${idPrefix}:decision_mood:${normalizedDecision.id.toLowerCase()}`,
      reason: `decision:${normalizedOption.key}`
    })
    const moodSummary = Object.entries(effects.mood)
      .map(([key, value]) => `${key}${Number(value) > 0 ? '+' : ''}${value}`)
      .join(',')
    if (moodSummary) summary.push(`mood(${moodSummary})`)
  }

  if (Number.isInteger(effects.threat_delta)) {
    const threat = ensureWorldThreat(memory.world)
    const current = Number(threat.byTown[normalizedDecision.town] || 0)
    const next = clamp(current + Number(effects.threat_delta), 0, 100)
    threat.byTown[normalizedDecision.town] = next
    summary.push(`threat=${next}`)
  }

  if (effects.rep_delta && typeof effects.rep_delta === 'object') {
    const towns = deriveTownsFromMarkers(memory.world?.markers || [])
    const town = findTownByName(towns, normalizedDecision.town)
    const markerName = asText(town?.marker?.name, '', 80)
    for (const [agentName, record] of Object.entries(memory.agents || {})) {
      const homeMarker = asText(record?.profile?.job?.home_marker, '', 80)
      if (markerName && !sameText(homeMarker, markerName, 80)) continue
      const profile = ensureAgentStoryProfile(ensureAgentProfile(memory, agentName))
      const rep = normalizeAgentRep(profile.rep)
      let changed = false
      for (const [factionName, delta] of Object.entries(effects.rep_delta)) {
        if (!STORY_FACTION_NAME_SET.has(factionName)) continue
        if (!Number.isInteger(delta) || delta === 0) continue
        rep[factionName] = Number(rep[factionName] || 0) + delta
        changed = true
      }
      if (!changed) continue
      profile.rep = rep
      applyRepThresholdTitleAwards(memory, agentName, at, `${idPrefix}:decision_rep:${normalizedDecision.id.toLowerCase()}`, normalizedDecision.town)
    }
  }

  let spawnedRumor = null
  if (effects.rumor_spawn) {
    spawnedRumor = spawnWorldRumor(memory, {
      townName: normalizedDecision.town,
      kind: effects.rumor_spawn.kind,
      severity: effects.rumor_spawn.severity,
      templateKey: effects.rumor_spawn.templateKey,
      expiresInDays: effects.rumor_spawn.expiresInDays,
      at,
      idPrefix: `${idPrefix}:decision_rumor:${normalizedDecision.id.toLowerCase()}`,
      spawnedByEventId: normalizedDecision.event_id
    })
    summary.push(`rumor=${spawnedRumor.id}`)
  }

  return {
    effects,
    spawnedRumor,
    summary
  }
}

/**
 * @param {any} memory
 * @param {number} day
 */
function expireRumorsForDay(memory, day) {
  const rumors = ensureWorldRumors(memory.world)
  memory.world.rumors = rumors.filter(rumor => Number(rumor.expires_day || 0) >= day)
  return memory.world.rumors.length
}

/**
 * @param {any} memory
 * @param {number} day
 */
function expireDecisionsForDay(memory, day) {
  const decisions = ensureWorldDecisions(memory.world)
  for (let idx = 0; idx < decisions.length; idx += 1) {
    const decision = normalizeDecision(decisions[idx])
    if (!decision) continue
    if (decision.state !== 'open') continue
    if (decision.expires_day < day) {
      decisions[idx] = { ...decision, state: 'expired' }
    }
  }
  return decisions
}

/**
 * @param {any} world
 */
function ensureWorldChronicle(world) {
  world.chronicle = normalizeWorldChronicle(world.chronicle)
  return world.chronicle
}

/**
 * @param {any} world
 */
function ensureWorldNews(world) {
  world.news = normalizeWorldNews(world.news)
  return world.news
}

/**
 * @param {any[]} list
 * @param {any} entry
 * @param {number} maxLen
 */
function appendBounded(list, entry, maxLen) {
  list.push(entry)
  if (list.length > maxLen) list.splice(0, list.length - maxLen)
}

/**
 * @param {any} memory
 * @param {any} entry
 */
function appendChronicle(memory, entry) {
  const normalized = normalizeChronicleEntry(entry)
  if (!normalized) return null
  const chronicle = ensureWorldChronicle(memory.world)
  appendBounded(chronicle, normalized, FEED_MAX_ENTRIES)
  return normalized
}

/**
 * @param {any} memory
 * @param {any} entry
 */
function appendNews(memory, entry) {
  const normalized = normalizeNewsEntry(entry)
  if (!normalized) return null
  const news = ensureWorldNews(memory.world)
  appendBounded(news, normalized, FEED_MAX_ENTRIES)
  return normalized
}

/**
 * @param {unknown} valueA
 * @param {unknown} valueB
 * @param {number} maxLen
 */
function sameText(valueA, valueB, maxLen) {
  const a = asText(valueA, '', maxLen).toLowerCase()
  const b = asText(valueB, '', maxLen).toLowerCase()
  return !!a && !!b && a === b
}

/**
 * @param {unknown} tag
 */
function parseTownNameFromTag(tag) {
  const safeTag = asText(tag, '', 80)
  if (!safeTag) return ''
  const match = /^(town|settlement)\s*:\s*(.+)$/i.exec(safeTag)
  if (!match) return ''
  return asText(match[2], '', 80)
}

/**
 * @param {unknown[]} markersInput
 */
function deriveTownsFromMarkers(markersInput) {
  const byTown = new Map()
  for (const entry of markersInput || []) {
    const marker = normalizeMarker(entry)
    if (!marker) continue
    const townName = parseTownNameFromTag(marker.tag)
    if (!townName) continue
    const key = townName.toLowerCase()
    if (!byTown.has(key)) byTown.set(key, { townName, marker })
  }
  return Array.from(byTown.values())
    .sort((a, b) => a.townName.localeCompare(b.townName))
}

/**
 * @param {{townName: string, marker: any}[]} towns
 * @param {string} townName
 */
function findTownByName(towns, townName) {
  const target = asText(townName, '', 80).toLowerCase()
  if (!target) return null
  for (const town of towns) {
    if (asText(town?.townName, '', 80).toLowerCase() === target) return town
  }
  return null
}

/**
 * @param {any} world
 */
function buildTownNameIndex(world) {
  const names = new Map()
  for (const townRaw of Object.keys(normalizeWorldTownMissionStates(world?.towns))) {
    const safeName = asText(townRaw, '', 80)
    if (!safeName) continue
    names.set(safeName.toLowerCase(), safeName)
  }
  for (const mission of normalizeWorldMajorMissions(world?.majorMissions)) {
    const safeName = asText(mission?.townId, '', 80)
    if (!safeName) continue
    const key = safeName.toLowerCase()
    if (!names.has(key)) names.set(key, safeName)
  }
  for (const town of deriveTownsFromMarkers(world?.markers || [])) {
    const safeName = asText(town?.townName, '', 80)
    if (!safeName) continue
    names.set(safeName.toLowerCase(), safeName)
  }
  const threat = normalizeWorldThreat(world?.threat)
  for (const townRaw of Object.keys(threat.byTown || {})) {
    const safeName = asText(townRaw, '', 80)
    if (!safeName) continue
    const key = safeName.toLowerCase()
    if (!names.has(key)) names.set(key, safeName)
  }
  const moods = normalizeWorldMoods(world?.moods)
  for (const townRaw of Object.keys(moods.byTown || {})) {
    const safeName = asText(townRaw, '', 80)
    if (!safeName) continue
    const key = safeName.toLowerCase()
    if (!names.has(key)) names.set(key, safeName)
  }
  return names
}

/**
 * @param {any} world
 * @param {string} townName
 */
function resolveTownName(world, townName) {
  const target = asText(townName, '', 80).toLowerCase()
  if (!target) return ''
  const byTown = buildTownNameIndex(world)
  return byTown.get(target) || ''
}

/**
 * @param {any[]} markers
 * @param {string | null} markerName
 */
function findTownNameForMarker(markers, markerName) {
  if (!markerName) return ''
  const marker = findMarkerByName(markers || [], markerName)
  if (!marker) return ''
  return parseTownNameFromTag(marker.tag)
}

/**
 * @param {any} entry
 * @param {string} townName
 */
function chronicleMentionsTown(entry, townName) {
  const town = asText(townName, '', 80).toLowerCase()
  if (!town) return false
  const entryTown = asText(entry?.town, '', 80).toLowerCase()
  if (entryTown && entryTown === town) return true
  const message = asText(entry?.msg, '', 240).toLowerCase()
  return !!message && message.includes(town)
}

/**
 * @param {unknown} value
 */
function asPositiveIntegerAmount(value) {
  const amount = asNumber(value)
  if (amount === null) return null
  if (!Number.isInteger(amount) || amount <= 0) return null
  return amount
}

/**
 * @param {string} rawCommand
 */
function parseGodCommand(rawCommand) {
  const full = asText(rawCommand, '', 240)
  if (!full) return { type: 'invalid', reason: 'No god command provided.' }
  const words = full.split(/\s+/)
  const head = words[0].toLowerCase()

  if (SUPPORTED_GOD_COMMANDS.has(head) && words.length === 1) {
    return { type: 'legacy_world', command: head }
  }

  if (head === 'loop') {
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'start') {
      const tickMsRaw = words[2]
      const tickMs = tickMsRaw ? Number(tickMsRaw) : undefined
      if (tickMsRaw && (!Number.isInteger(tickMs) || tickMs < 100)) {
        return { type: 'invalid', reason: 'god loop start tickMs must be an integer >= 100.' }
      }
      return { type: 'loop_start', tickMs }
    }
    if (action === 'stop') return { type: 'loop_stop' }
    if (action === 'status') return { type: 'loop_status' }
    return { type: 'invalid', reason: 'Usage: god loop start [tickMs] | god loop stop | god loop status' }
  }

  if (head === 'status') return { type: 'status' }

  if (head === 'inspect') {
    const target = asText(words[1], '', 80)
    if (!target) return { type: 'invalid', reason: 'Usage: god inspect <agent|world>' }
    if (target.toLowerCase() === 'world') return { type: 'inspect_world' }
    return { type: 'inspect_agent', name: target }
  }

  if (head === 'leader') {
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'set') {
      const name = asText(words.slice(2).join(' '), '', 80)
      if (!name) return { type: 'invalid', reason: 'Usage: god leader set <name>' }
      return { type: 'leader_set', name }
    }
    if (action === 'clear') return { type: 'leader_clear' }
    return { type: 'invalid', reason: 'Usage: god leader set <name> | god leader clear' }
  }

  if (head === 'freeze') {
    const name = asText(words.slice(1).join(' '), '', 80)
    if (!name) return { type: 'invalid', reason: 'Usage: god freeze <agent>' }
    return { type: 'freeze', name }
  }

  if (head === 'unfreeze') {
    const name = asText(words.slice(1).join(' '), '', 80)
    if (!name) return { type: 'invalid', reason: 'Usage: god unfreeze <agent>' }
    return { type: 'unfreeze', name }
  }

  if (head === 'intent') {
    const action = asText(words[1], '', 20).toLowerCase()
    const agentName = asText(words[2], '', 80)
    const intent = asText(words[3], '', 16).toLowerCase()
    const target = asText(words.slice(4).join(' '), '', 80) || null
    if (action !== 'set' || !agentName || !intent || !INTENT_TYPES.has(intent)) {
      return { type: 'invalid', reason: 'Usage: god intent set <agent> <idle|wander|follow|respond> [target]' }
    }
    return { type: 'intent_set', name: agentName, intent, target }
  }

  if (head === 'mark') {
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'list') return { type: 'mark_list' }
    if (action === 'remove') {
      const name = asText(words[2], '', 80)
      if (!name) return { type: 'invalid', reason: 'Usage: god mark remove <name>' }
      return { type: 'mark_remove', name }
    }
    if (action === 'add') {
      const name = asText(words[2], '', 80)
      const x = asNumber(words[3])
      const y = asNumber(words[4])
      const z = asNumber(words[5])
      const tag = asText(words.slice(6).join(' '), '', 80)
      if (!name || x === null || y === null || z === null) {
        return { type: 'invalid', reason: 'Usage: god mark add <name> <x> <y> <z> [tag]' }
      }
      return { type: 'mark_add', name, x, y, z, tag }
    }
    return { type: 'invalid', reason: 'Usage: god mark add <name> <x> <y> <z> [tag] | god mark list | god mark remove <name>' }
  }

  if (head === 'job') {
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'roster') return { type: 'job_roster' }
    if (action === 'clear') {
      const name = asText(words[2], '', 80)
      if (!name) return { type: 'invalid', reason: 'Usage: god job clear <agent>' }
      return { type: 'job_clear', name }
    }
    if (action === 'set') {
      const name = asText(words[2], '', 80)
      const role = asText(words[3], '', 30).toLowerCase()
      const marker = asText(words.slice(4).join(' '), '', 80) || null
      if (!name || !role) return { type: 'invalid', reason: 'Usage: god job set <agent> <role> [marker]' }
      return { type: 'job_set', name, role, marker }
    }
    return { type: 'invalid', reason: 'Usage: god job set <agent> <role> [marker] | god job clear <agent> | god roster' }
  }

  if (head === 'roster' && words.length === 1) return { type: 'job_roster' }

  if (head === 'mint') {
    const name = asText(words[1], '', 80)
    const amount = asNumber(words[2])
    if (!name || words.length !== 3) return { type: 'invalid', reason: 'Usage: god mint <agent> <amount>' }
    return { type: 'economy_mint', name, amount }
  }

  if (head === 'transfer') {
    const from = asText(words[1], '', 80)
    const to = asText(words[2], '', 80)
    const amount = asNumber(words[3])
    if (!from || !to || words.length !== 4) return { type: 'invalid', reason: 'Usage: god transfer <fromAgent> <toAgent> <amount>' }
    return { type: 'economy_transfer', from, to, amount }
  }

  if (head === 'balance') {
    const name = asText(words[1], '', 80)
    if (!name || words.length !== 2) return { type: 'invalid', reason: 'Usage: god balance <agent>' }
    return { type: 'economy_balance', name }
  }

  if (head === 'economy' && words.length === 1) return { type: 'economy_overview' }

  if (head === 'town') {
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'list' && words.length === 2) return { type: 'town_list' }
    if (action === 'board') {
      if (words.length < 3) return { type: 'invalid', reason: 'Usage: god town board <townName> [N]' }
      let limit = 10
      let nameWords = words.slice(2)
      if (nameWords.length >= 2) {
        const maybeLimit = Number(nameWords[nameWords.length - 1])
        if (Number.isInteger(maybeLimit) && maybeLimit > 0) {
          limit = maybeLimit
          nameWords = nameWords.slice(0, -1)
        }
      }
      const townName = asText(nameWords.join(' '), '', 80)
      if (!townName) return { type: 'invalid', reason: 'Usage: god town board <townName> [N]' }
      return { type: 'town_board', townName, limit }
    }
    return { type: 'invalid', reason: 'Usage: god town list | god town board <townName> [N]' }
  }

  if (head === 'mayor') {
    const action = asText(words[1], '', 20).toLowerCase()
    const townName = asText(words.slice(2).join(' '), '', 80)
    if (!townName || words.length < 3) {
      return { type: 'invalid', reason: 'Usage: god mayor talk <townName> | god mayor accept <townName>' }
    }
    if (action === 'talk') return { type: 'mayor_talk', townName }
    if (action === 'accept') return { type: 'mayor_accept', townName }
    return { type: 'invalid', reason: 'Usage: god mayor talk <townName> | god mayor accept <townName>' }
  }

  if (head === 'mission') {
    const action = asText(words[1], '', 20).toLowerCase()
    const townName = asText(words[2], '', 80)
    if (!townName || words.length < 3) {
      return { type: 'invalid', reason: 'Usage: god mission status <townName> | god mission advance <townName> | god mission complete <townName> | god mission fail <townName> [reason]' }
    }
    if (action === 'status' && words.length === 3) return { type: 'mission_status', townName }
    if (action === 'advance' && words.length === 3) return { type: 'mission_advance', townName }
    if (action === 'complete' && words.length === 3) return { type: 'mission_complete', townName }
    if (action === 'fail' && words.length >= 3) {
      const reason = asText(words.slice(3).join(' '), '', 160) || null
      return { type: 'mission_fail', townName, reason }
    }
    return { type: 'invalid', reason: 'Usage: god mission status <townName> | god mission advance <townName> | god mission complete <townName> | god mission fail <townName> [reason]' }
  }

  if (head === 'nether') {
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'status' && words.length === 2) return { type: 'nether_status' }
    if (action === 'tick') {
      if (words.length > 3) return { type: 'invalid', reason: 'Usage: god nether status | god nether tick [nDays]' }
      const nDays = words[2] === undefined ? 1 : Number(words[2])
      if (!Number.isInteger(nDays) || nDays < 1) {
        return { type: 'invalid', reason: 'Usage: god nether status | god nether tick [nDays]' }
      }
      return { type: 'nether_tick', nDays }
    }
    return { type: 'invalid', reason: 'Usage: god nether status | god nether tick [nDays]' }
  }

  if (head === 'townsfolk') {
    const action = asText(words[1], '', 20).toLowerCase()
    const townName = asText(words[2], '', 80)
    const npcName = asText(words.slice(3).join(' '), '', 80)
    if (action === 'talk' && townName && npcName) {
      return { type: 'townsfolk_talk', townName, npcName }
    }
    return { type: 'invalid', reason: 'Usage: god townsfolk talk <townName> <npcIdOrName>' }
  }

  if (head === 'clock') {
    if (words.length === 1) return { type: 'clock_show' }
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'advance') {
      if (words.length > 3) return { type: 'invalid', reason: 'Usage: god clock | god clock advance [ticks] | god clock season <dawn|long_night>' }
      const ticks = words[2] === undefined ? 1 : Number(words[2])
      if (!Number.isInteger(ticks) || ticks < 1) {
        return { type: 'invalid', reason: 'Usage: god clock | god clock advance [ticks] | god clock season <dawn|long_night>' }
      }
      return { type: 'clock_advance', ticks }
    }
    if (action === 'season') {
      const season = asText(words[2], '', 20).toLowerCase()
      if (!season || words.length !== 3) {
        return { type: 'invalid', reason: 'Usage: god clock | god clock advance [ticks] | god clock season <dawn|long_night>' }
      }
      return { type: 'clock_season', season }
    }
    return { type: 'invalid', reason: 'Usage: god clock | god clock advance [ticks] | god clock season <dawn|long_night>' }
  }

  if (head === 'threat') {
    if (words.length === 1) return { type: 'threat_list' }
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'set') {
      const townName = asText(words[2], '', 80)
      const value = asNumber(words[3])
      if (!townName || words.length !== 4) {
        return { type: 'invalid', reason: 'Usage: god threat [townName] | god threat set <townName> <value>' }
      }
      return { type: 'threat_set', townName, value }
    }
    if (words.length === 2) {
      const townName = asText(words[1], '', 80)
      if (!townName) return { type: 'invalid', reason: 'Usage: god threat [townName] | god threat set <townName> <value>' }
      return { type: 'threat_show', townName }
    }
    return { type: 'invalid', reason: 'Usage: god threat [townName] | god threat set <townName> <value>' }
  }

  if (head === 'mood') {
    if (words.length === 1) return { type: 'mood_list' }
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'list') {
      if (words.length !== 2) return { type: 'invalid', reason: 'Usage: god mood [townName] | god mood list' }
      return { type: 'mood_list' }
    }
    if (words.length === 2) {
      const townName = asText(words[1], '', 80)
      if (!townName) return { type: 'invalid', reason: 'Usage: god mood [townName] | god mood list' }
      return { type: 'mood_show', townName }
    }
    return { type: 'invalid', reason: 'Usage: god mood [townName] | god mood list' }
  }

  if (head === 'event') {
    if (words.length === 1) return { type: 'event_list' }
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'list') {
      if (words.length !== 2) return { type: 'invalid', reason: 'Usage: god event | god event list | god event seed <int> | god event draw [townName] | god event clear <eventId>' }
      return { type: 'event_list' }
    }
    if (action === 'seed') {
      const seed = asNumber(words[2])
      if (words.length !== 3 || seed === null) {
        return { type: 'invalid', reason: 'Usage: god event seed <int>' }
      }
      return { type: 'event_seed', seed }
    }
    if (action === 'draw') {
      if (words.length > 3) {
        return { type: 'invalid', reason: 'Usage: god event draw [townName]' }
      }
      const townName = words[2] === undefined ? null : asText(words[2], '', 80)
      if (words[2] !== undefined && !townName) {
        return { type: 'invalid', reason: 'Usage: god event draw [townName]' }
      }
      return { type: 'event_draw', townName }
    }
    if (action === 'clear') {
      const eventId = asText(words[2], '', 200)
      if (!eventId || words.length !== 3) {
        return { type: 'invalid', reason: 'Usage: god event clear <eventId>' }
      }
      return { type: 'event_clear', eventId }
    }
    return { type: 'invalid', reason: 'Usage: god event | god event list | god event seed <int> | god event draw [townName] | god event clear <eventId>' }
  }

  if (head === 'faction') {
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'list' && words.length === 2) return { type: 'faction_list' }
    if (action === 'set') {
      const townName = asText(words[2], '', 80)
      const factionName = asText(words[3], '', 80).toLowerCase()
      if (!townName || !factionName || words.length !== 4) {
        return { type: 'invalid', reason: 'Usage: god faction list | god faction set <townName> <factionName>' }
      }
      return { type: 'faction_set', townName, factionName }
    }
    return { type: 'invalid', reason: 'Usage: god faction list | god faction set <townName> <factionName>' }
  }

  if (head === 'rep') {
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'add') {
      const agentName = asText(words[2], '', 80)
      const factionName = asText(words[3], '', 80).toLowerCase()
      const delta = asNumber(words[4])
      if (!agentName || !factionName || words.length !== 5) {
        return { type: 'invalid', reason: 'Usage: god rep <agent> [factionName] | god rep add <agent> <factionName> <deltaInt>' }
      }
      return { type: 'rep_add', agentName, factionName, delta }
    }
    const agentName = asText(words[1], '', 80)
    const factionName = asText(words[2], '', 80).toLowerCase() || null
    if (!agentName || words.length > 3) {
      return { type: 'invalid', reason: 'Usage: god rep <agent> [factionName] | god rep add <agent> <factionName> <deltaInt>' }
    }
    return { type: 'rep_show', agentName, factionName }
  }

  if (head === 'rumor') {
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'list') {
      if (words.length > 4) {
        return { type: 'invalid', reason: 'Usage: god rumor list [townName] [limit]' }
      }
      let townName = null
      let limit = 10
      if (words[2] !== undefined) {
        const maybeLimit = Number(words[2])
        if (Number.isInteger(maybeLimit) && maybeLimit > 0) {
          limit = maybeLimit
        } else {
          townName = asText(words[2], '', 80) || null
        }
      }
      if (words[3] !== undefined) {
        const parsedLimit = Number(words[3])
        if (!Number.isInteger(parsedLimit) || parsedLimit <= 0) {
          return { type: 'invalid', reason: 'Usage: god rumor list [townName] [limit]' }
        }
        limit = parsedLimit
      }
      return { type: 'rumor_list', townName, limit }
    }
    if (action === 'show') {
      const rumorId = asText(words[2], '', 200)
      if (!rumorId || words.length !== 3) {
        return { type: 'invalid', reason: 'Usage: god rumor show <rumorId>' }
      }
      return { type: 'rumor_show', rumorId }
    }
    if (action === 'spawn') {
      const townName = asText(words[2], '', 80)
      const kind = asText(words[3], '', 20).toLowerCase()
      const severity = Number(words[4])
      const templateKey = asText(words[5], '', 80).toLowerCase()
      const expiresInDays = words[6] === undefined ? null : Number(words[6])
      if (!townName || !kind || !templateKey || words.length < 6 || words.length > 7) {
        return { type: 'invalid', reason: 'Usage: god rumor spawn <town> <kind> <severity> <templateKey> [expiresInDays]' }
      }
      return { type: 'rumor_spawn', townName, kind, severity, templateKey, expiresInDays }
    }
    if (action === 'clear') {
      const rumorId = asText(words[2], '', 200)
      if (!rumorId || words.length !== 3) {
        return { type: 'invalid', reason: 'Usage: god rumor clear <rumorId>' }
      }
      return { type: 'rumor_clear', rumorId }
    }
    if (action === 'resolve') {
      const rumorId = asText(words[2], '', 200)
      const questId = asText(words[3], '', 200)
      if (!rumorId || !questId || words.length !== 4) {
        return { type: 'invalid', reason: 'Usage: god rumor resolve <rumorId> <questId>' }
      }
      return { type: 'rumor_resolve', rumorId, questId }
    }
    if (action === 'quest') {
      const rumorId = asText(words[2], '', 200)
      if (!rumorId || words.length !== 3) {
        return { type: 'invalid', reason: 'Usage: god rumor quest <rumorId>' }
      }
      return { type: 'rumor_quest', rumorId }
    }
    return { type: 'invalid', reason: 'Usage: god rumor list [townName] [limit] | god rumor show <rumorId> | god rumor spawn <town> <kind> <severity> <templateKey> [expiresInDays] | god rumor clear <rumorId> | god rumor resolve <rumorId> <questId> | god rumor quest <rumorId>' }
  }

  if (head === 'decision') {
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'list') {
      if (words.length > 3) return { type: 'invalid', reason: `Usage: god decision list [town]. ${DECISION_DEPRECATION_NOTE}` }
      const townName = asText(words[2], '', 80) || null
      return { type: 'decision_list', townName }
    }
    if (action === 'show') {
      const decisionId = asText(words[2], '', 200)
      if (!decisionId || words.length !== 3) return { type: 'invalid', reason: `Usage: god decision show <decisionId>. ${DECISION_DEPRECATION_NOTE}` }
      return { type: 'decision_show', decisionId }
    }
    if (action === 'choose') {
      const decisionId = asText(words[2], '', 200)
      const optionKey = asText(words[3], '', 40).toLowerCase()
      if (!decisionId || !optionKey || words.length !== 4) return { type: 'invalid', reason: `Usage: god decision choose <decisionId> <optionKey>. ${DECISION_DEPRECATION_NOTE}` }
      return { type: 'decision_choose', decisionId, optionKey }
    }
    if (action === 'expire') {
      const decisionId = asText(words[2], '', 200)
      if (!decisionId || words.length !== 3) return { type: 'invalid', reason: `Usage: god decision expire <decisionId>. ${DECISION_DEPRECATION_NOTE}` }
      return { type: 'decision_expire', decisionId }
    }
    return { type: 'invalid', reason: `Usage: god decision list [town] | god decision show <decisionId> | god decision choose <decisionId> <optionKey> | god decision expire <decisionId>. ${DECISION_DEPRECATION_NOTE}` }
  }

  if (head === 'trait') {
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'set') {
      const agentName = asText(words[2], '', 80)
      const traitName = asText(words[3], '', 20).toLowerCase()
      const value = Number(words[4])
      if (!agentName || !traitName || words.length !== 5) {
        return { type: 'invalid', reason: 'Usage: god trait <agent> | god trait set <agent> <traitName> <0-3>' }
      }
      return { type: 'trait_set', agentName, traitName, value }
    }
    const agentName = asText(words[1], '', 80)
    if (!agentName || words.length !== 2) {
      return { type: 'invalid', reason: 'Usage: god trait <agent> | god trait set <agent> <traitName> <0-3>' }
    }
    return { type: 'trait_show', agentName }
  }

  if (head === 'title') {
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'grant') {
      const agentName = asText(words[2], '', 80)
      const title = asText(words.slice(3).join(' '), '', MAX_AGENT_TITLE_LEN)
      if (!agentName || !title || words.length < 4) {
        return { type: 'invalid', reason: 'Usage: god title <agent> | god title grant <agent> <title> | god title revoke <agent> <title>' }
      }
      return { type: 'title_grant', agentName, title }
    }
    if (action === 'revoke') {
      const agentName = asText(words[2], '', 80)
      const title = asText(words.slice(3).join(' '), '', MAX_AGENT_TITLE_LEN)
      if (!agentName || !title || words.length < 4) {
        return { type: 'invalid', reason: 'Usage: god title <agent> | god title grant <agent> <title> | god title revoke <agent> <title>' }
      }
      return { type: 'title_revoke', agentName, title }
    }
    const agentName = asText(words[1], '', 80)
    if (!agentName || words.length !== 2) {
      return { type: 'invalid', reason: 'Usage: god title <agent> | god title grant <agent> <title> | god title revoke <agent> <title>' }
    }
    return { type: 'title_show', agentName }
  }

  if (head === 'chronicle') {
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'tail') {
      if (words.length > 3) return { type: 'invalid', reason: 'Usage: god chronicle tail [N]' }
      const limit = words[2] === undefined ? 10 : Number(words[2])
      if (!Number.isInteger(limit) || limit <= 0) return { type: 'invalid', reason: 'Usage: god chronicle tail [N]' }
      return { type: 'chronicle_tail', limit }
    }
    if (action === 'grep') {
      if (words.length < 3) return { type: 'invalid', reason: 'Usage: god chronicle grep <term> [N]' }
      let limit = 10
      let termWords = words.slice(2)
      if (termWords.length >= 2) {
        const maybeLimit = Number(termWords[termWords.length - 1])
        if (Number.isInteger(maybeLimit) && maybeLimit > 0) {
          limit = maybeLimit
          termWords = termWords.slice(0, -1)
        }
      }
      const term = asText(termWords.join(' '), '', 120)
      if (!term) return { type: 'invalid', reason: 'Usage: god chronicle grep <term> [N]' }
      return { type: 'chronicle_grep', term, limit }
    }
    if (action === 'add') {
      const eventType = asText(words[2], '', 40).toLowerCase()
      const tokens = words.slice(3).map(item => asText(item, '', 120)).filter(Boolean)
      if (!eventType || tokens.length === 0) {
        return { type: 'invalid', reason: 'Usage: god chronicle add <type> <message...> [townName|town=<townName>]' }
      }
      return { type: 'chronicle_add', eventType, tokens }
    }
    return { type: 'invalid', reason: 'Usage: god chronicle add <type> <message...> [townName] | god chronicle tail [N] | god chronicle grep <term> [N]' }
  }

  if (head === 'news') {
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'tail') {
      if (words.length > 3) return { type: 'invalid', reason: 'Usage: god news tail [N]' }
      const limit = words[2] === undefined ? 10 : Number(words[2])
      if (!Number.isInteger(limit) || limit <= 0) return { type: 'invalid', reason: 'Usage: god news tail [N]' }
      return { type: 'news_tail', limit }
    }
    return { type: 'invalid', reason: 'Usage: god news tail [N]' }
  }

  if (head === 'quest') {
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'offer') {
      const sourceTown = asText(words[2], '', 80)
      const questType = asText(words[3], '', 20).toLowerCase()
      if (!sourceTown || !questType) {
        return { type: 'invalid', reason: 'Usage: god quest offer <townName> trade_n <n> [marketName] [reward] | god quest offer <townName> visit_town <townName> [reward]' }
      }
      if (questType === 'trade_n') {
        const n = Number(words[4])
        const extras = words.slice(5)
        if (words.length < 5 || extras.length > 2) {
          return { type: 'invalid', reason: 'Usage: god quest offer <townName> trade_n <n> [marketName] [reward]' }
        }
        let marketName = null
        let reward = null
        if (extras.length === 1) {
          const maybeReward = Number(extras[0])
          if (Number.isInteger(maybeReward) && maybeReward >= 0) reward = maybeReward
          else marketName = asText(extras[0], '', 80) || null
        }
        if (extras.length === 2) {
          marketName = asText(extras[0], '', 80) || null
          reward = Number(extras[1])
        }
        return { type: 'quest_offer_trade_n', sourceTown, n, marketName, reward }
      }
      if (questType === 'visit_town') {
        if (words.length < 5 || words.length > 6) {
          return { type: 'invalid', reason: 'Usage: god quest offer <townName> visit_town <townName> [reward]' }
        }
        const targetTown = asText(words[4], '', 80)
        const reward = words[5] === undefined ? null : Number(words[5])
        if (!targetTown) {
          return { type: 'invalid', reason: 'Usage: god quest offer <townName> visit_town <townName> [reward]' }
        }
        return { type: 'quest_offer_visit_town', sourceTown, targetTown, reward }
      }
      return { type: 'invalid', reason: 'Usage: god quest offer <townName> trade_n <n> [marketName] [reward] | god quest offer <townName> visit_town <townName> [reward]' }
    }
    if (action === 'accept') {
      const agentName = asText(words[2], '', 80)
      const questId = asText(words[3], '', 200)
      if (!agentName || !questId || words.length !== 4) {
        return { type: 'invalid', reason: 'Usage: god quest accept <agent> <questId>' }
      }
      return { type: 'quest_accept', agentName, questId }
    }
    if (action === 'cancel') {
      const questId = asText(words[2], '', 200)
      if (!questId || words.length !== 3) {
        return { type: 'invalid', reason: 'Usage: god quest cancel <questId>' }
      }
      return { type: 'quest_cancel', questId }
    }
    if (action === 'complete') {
      const questId = asText(words[2], '', 200)
      if (!questId || words.length !== 3) {
        return { type: 'invalid', reason: 'Usage: god quest complete <questId>' }
      }
      return { type: 'quest_complete', questId }
    }
    if (action === 'visit') {
      const questId = asText(words[2], '', 200)
      if (!questId || words.length !== 3) {
        return { type: 'invalid', reason: 'Usage: god quest visit <questId>' }
      }
      return { type: 'quest_visit', questId }
    }
    if (action === 'list') {
      if (words.length > 3) return { type: 'invalid', reason: 'Usage: god quest list [townName]' }
      const townName = asText(words[2], '', 80) || null
      return { type: 'quest_list', townName }
    }
    if (action === 'show') {
      const questId = asText(words[2], '', 200)
      if (!questId || words.length !== 3) {
        return { type: 'invalid', reason: 'Usage: god quest show <questId>' }
      }
      return { type: 'quest_show', questId }
    }
    return { type: 'invalid', reason: 'Usage: god quest offer <townName> trade_n <n> [marketName] [reward] | god quest offer <townName> visit_town <townName> [reward] | god quest accept <agent> <questId> | god quest cancel <questId> | god quest complete <questId> | god quest visit <questId> | god quest list [townName] | god quest show <questId>' }
  }

  if (head === 'market') {
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'list' && words.length === 2) return { type: 'market_list' }
    if (action === 'pulse') {
      if (words.length !== 3) return { type: 'invalid', reason: 'Usage: god market pulse <townName|world>' }
      if (sameText(words[2], 'world', 20)) return { type: 'market_pulse_world' }
      const townName = asText(words[2], '', 80)
      if (!townName) return { type: 'invalid', reason: 'Usage: god market pulse <townName|world>' }
      return { type: 'market_pulse_town', townName }
    }
    if (action === 'add') {
      const name = asText(words[2], '', 80)
      const marker = asText(words.slice(3).join(' '), '', 80) || null
      if (!name || words.length < 3) return { type: 'invalid', reason: 'Usage: god market add <marketName> [marker]' }
      return { type: 'market_add', name, marker }
    }
    if (action === 'remove') {
      const name = asText(words[2], '', 80)
      if (!name || words.length !== 3) return { type: 'invalid', reason: 'Usage: god market remove <marketName>' }
      return { type: 'market_remove', name }
    }
    return { type: 'invalid', reason: 'Usage: god market add <marketName> [marker] | god market remove <marketName> | god market list | god market pulse <townName|world>' }
  }

  if (head === 'offer') {
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'list') {
      const marketName = asText(words[2], '', 80)
      if (!marketName || words.length !== 3) return { type: 'invalid', reason: 'Usage: god offer list <marketName>' }
      return { type: 'offer_list', marketName }
    }
    if (action === 'add') {
      const marketName = asText(words[2], '', 80)
      const owner = asText(words[3], '', 80)
      const side = asText(words[4], '', 20).toLowerCase()
      const amount = asNumber(words[5])
      const price = asNumber(words[6])
      if (!marketName || !owner || !side || words.length !== 7) {
        return { type: 'invalid', reason: 'Usage: god offer add <marketName> <owner> <buy|sell> <amount> <price>' }
      }
      return { type: 'offer_add', marketName, owner, side, amount, price }
    }
    if (action === 'cancel') {
      const marketName = asText(words[2], '', 80)
      const offerId = asText(words[3], '', 160)
      if (!marketName || !offerId || words.length !== 4) {
        return { type: 'invalid', reason: 'Usage: god offer cancel <marketName> <offer_id>' }
      }
      return { type: 'offer_cancel', marketName, offerId }
    }
    return { type: 'invalid', reason: 'Usage: god offer add <marketName> <owner> <buy|sell> <amount> <price> | god offer cancel <marketName> <offer_id> | god offer list <marketName>' }
  }

  if (head === 'trade') {
    const marketName = asText(words[1], '', 80)
    const offerId = asText(words[2], '', 160)
    const buyer = asText(words[3], '', 80)
    const amount = asNumber(words[4])
    if (!marketName || !offerId || !buyer || words.length !== 5) {
      return { type: 'invalid', reason: 'Usage: god trade <marketName> <offer_id> <buyer> <amount>' }
    }
    return { type: 'market_trade', marketName, offerId, buyer, amount }
  }

  if (head === 'contract') {
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'list') {
      if (words.length > 3) return { type: 'invalid', reason: 'Usage: god contract list [townName]' }
      const townName = asText(words[2], '', 80) || null
      return { type: 'contract_list', townName }
    }
    if (action === 'show') {
      const questId = asText(words[2], '', 200)
      if (!questId || words.length !== 3) return { type: 'invalid', reason: 'Usage: god contract show <questId>' }
      return { type: 'contract_show', questId }
    }
    if (action === 'accept') {
      if (words.length === 3) {
        const questId = asText(words[2], '', 200)
        if (!questId) return { type: 'invalid', reason: 'Usage: god contract accept <questId> | god contract accept <agent> <questId>' }
        return { type: 'contract_accept', questId, agentName: null, autoAssign: true }
      }
      if (words.length === 4) {
        const agentName = asText(words[2], '', 80)
        const questId = asText(words[3], '', 200)
        if (!agentName || !questId) return { type: 'invalid', reason: 'Usage: god contract accept <questId> | god contract accept <agent> <questId>' }
        return { type: 'contract_accept', questId, agentName, autoAssign: false }
      }
      return { type: 'invalid', reason: 'Usage: god contract accept <questId> | god contract accept <agent> <questId>' }
    }
    if (action === 'complete') {
      const questId = asText(words[2], '', 200)
      if (!questId || words.length !== 3) return { type: 'invalid', reason: 'Usage: god contract complete <questId>' }
      return { type: 'contract_complete', questId }
    }
    return { type: 'invalid', reason: 'Usage: god contract list [townName] | god contract show <questId> | god contract accept <questId> | god contract accept <agent> <questId> | god contract complete <questId>' }
  }

  if (head === 'say') {
    const match = /^say\s+(\S+)\s+(.+)$/i.exec(full)
    if (!match) return { type: 'invalid', reason: 'Usage: god say <agent> <message>' }
    return {
      type: 'say',
      name: asText(match[1], '', 80),
      message: asText(match[2], '', 240)
    }
  }

  return { type: 'invalid', reason: `Unsupported god command: ${full}` }
}

/**
 * @param {ReturnType<import('./memory').createMemoryStore>} memoryStore
 * @param {unknown[]} agents
 * @param {ReturnType<import('./worldLoop').createWorldLoop> | null} worldLoop
 */
function buildDefaultStatus(memoryStore, agents, worldLoop) {
  const loopStatus = worldLoop && typeof worldLoop.getWorldLoopStatus === 'function'
    ? worldLoop.getWorldLoopStatus()
    : {
      running: false,
      tickMs: 0,
      lastTickAt: 0,
      scheduledCount: 0,
      backpressure: false,
      reason: 'not_configured'
    }
  const runtimeMetrics = memoryStore.getRuntimeMetrics()
  const observability = getObservabilitySnapshot()
  const snapshot = memoryStore.getSnapshot()
  const avgTxMs = observability.txDurationCount > 0
    ? observability.txDurationTotalMs / observability.txDurationCount
    : 0
  const slowTxRate = observability.txDurationCount > 0
    ? observability.slowTransactionCount / observability.txDurationCount
    : 0
  const heapMb = process.memoryUsage().heapUsed / (1024 * 1024)
  const memoryBytes = Buffer.byteLength(JSON.stringify(snapshot), 'utf-8')
  const integrity = memoryStore.validateMemoryIntegrity()
  const guardrailFlags = []
  if (!integrity.ok) guardrailFlags.push('CRITICAL:integrity_failed')
  if (runtimeMetrics.lockTimeouts > 0) guardrailFlags.push('CRITICAL:lock_timeouts')
  if (observability.txDurationP99Ms >= 500) guardrailFlags.push('CRITICAL:p99_tx_ge_500')
  if (slowTxRate > 0.10) guardrailFlags.push('WARN:slow_tx_rate_gt_0_10')
  if (loopStatus.backpressure) guardrailFlags.push(`WARN:backpressure:${loopStatus.reason}`)

  return {
    loopStatus,
    runtimeMetrics,
    observability,
    agentsOnline: agents.length,
    avgTxMs,
    p95TxMs: observability.txDurationP95Ms,
    p99TxMs: observability.txDurationP99Ms,
    lockWaitP95Ms: observability.txPhaseP95Ms.lockWaitMs,
    lockWaitP99Ms: observability.txPhaseP99Ms.lockWaitMs,
    memoryBytes,
    heapMb,
    guardrailFlags
  }
}

/**
 * @param {{
 *   memoryStore: ReturnType<import('./memory').createMemoryStore>,
 *   logger?: ReturnType<typeof createLogger>,
 *   worldLoop?: ReturnType<import('./worldLoop').createWorldLoop> | null,
 *   getStatusSnapshot?: (input: {agents: unknown[]}) => Promise<any> | any,
 *   runtimeSay?: (input: {agent: any, message: string}) => Promise<void> | void,
 *   runtimeMark?: (input: {action: 'add' | 'remove', markerName: string, marker?: any}) => Promise<void> | void,
 *   runtimeJob?: (input: {action: 'set' | 'clear', agentName: string, job?: {role: string, assigned_at: string, home_marker: string | null}}) => Promise<void> | void,
 *   now?: () => number
 * }} deps
 */
function createGodCommandService(deps) {
  if (!deps?.memoryStore) {
    throw new AppError({
      code: 'GOD_SERVICE_CONFIG_ERROR',
      message: 'memoryStore dependency is required for god command service.',
      recoverable: false
    })
  }

  const memoryStore = deps.memoryStore
  const logger = deps.logger || createLogger({ component: 'god_commands' })
  const worldLoop = deps.worldLoop || null
  const runtimeSay = typeof deps.runtimeSay === 'function' ? deps.runtimeSay : null
  const runtimeMark = typeof deps.runtimeMark === 'function' ? deps.runtimeMark : null
  const runtimeJob = typeof deps.runtimeJob === 'function' ? deps.runtimeJob : null
  const getStatusSnapshot = typeof deps.getStatusSnapshot === 'function' ? deps.getStatusSnapshot : null
  const now = deps.now || (() => Date.now())

  /**
   * @param {{agents: unknown[], command: string, operationId: string}} input
   */
  async function applyGodCommand(input) {
    const command = asText(input?.command, '', 240)
    const operationId = asText(input?.operationId, '', 200)
    const runtimeAgents = Array.isArray(input?.agents) ? input.agents.filter(isRuntimeAgentShape) : []
    const legacyAgents = runtimeAgents.filter(isLegacyGodAgentShape)
    const parsed = parseGodCommand(command)

    if (parsed.type === 'invalid') {
      throw new AppError({
        code: 'INVALID_GOD_COMMAND',
        message: parsed.reason,
        recoverable: true
      })
    }
    if (!operationId) {
      throw new AppError({
        code: 'INVALID_GOD_OPERATION',
        message: 'God command requires operationId for idempotency.',
        recoverable: true
      })
    }

    if (parsed.type === 'legacy_world') {
      const tx = await memoryStore.transact((memory) => {
        const world = memory.world
        const at = now()
        if (parsed.command === 'declare_war') world.warActive = true
        if (parsed.command === 'make_peace') world.warActive = false
        if (parsed.command === 'declare_war') world.player.legitimacy = Math.max(0, world.player.legitimacy - 8)
        if (parsed.command === 'bless_people') world.player.legitimacy = Math.min(100, world.player.legitimacy + 5)
        appendChronicle(memory, {
          id: `${operationId}:chronicle:god_command:${parsed.command}`,
          type: 'god_command',
          msg: `GOD EVENT: ${parsed.command}`,
          at,
          meta: { command: parsed.command }
        })
      }, { eventId: `${operationId}:god_command` })

      if (tx.skipped) {
        logger.info('god_command_duplicate_ignored', { operationId, command: parsed.command })
        return { applied: false, command, reason: 'Duplicate operation ignored.' }
      }

      // Persist world state before applying runtime side effects to prevent drift.
      legacyAgents.forEach(agent => agent.applyGodCommand(parsed.command))
      logger.info('god_command_applied', { operationId, command: parsed.command, affectedAgents: legacyAgents.length })
      return { applied: true, command, audit: true }
    }

    if (parsed.type === 'loop_start') {
      if (!worldLoop || typeof worldLoop.startWorldLoop !== 'function') {
        throw new AppError({
          code: 'WORLD_LOOP_UNAVAILABLE',
          message: 'World loop is not configured.',
          recoverable: true
        })
      }
      const status = worldLoop.startWorldLoop({ tickMs: parsed.tickMs })
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `WORLD LOOP STARTED: tickMs=${status.tickMs}`,
          `WORLD LOOP STATUS: running=${status.running} backpressure=${status.backpressure} reason=${status.reason}`
        ]
      }
    }

    if (parsed.type === 'loop_stop') {
      if (!worldLoop || typeof worldLoop.stopWorldLoop !== 'function') {
        throw new AppError({
          code: 'WORLD_LOOP_UNAVAILABLE',
          message: 'World loop is not configured.',
          recoverable: true
        })
      }
      const status = worldLoop.stopWorldLoop()
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `WORLD LOOP STOPPED: running=${status.running}`,
          `WORLD LOOP STATUS: running=${status.running} backpressure=${status.backpressure} reason=${status.reason}`
        ]
      }
    }

    if (parsed.type === 'loop_status') {
      const status = worldLoop && typeof worldLoop.getWorldLoopStatus === 'function'
        ? worldLoop.getWorldLoopStatus()
        : {
          running: false,
          tickMs: 0,
          lastTickAt: 0,
          scheduledCount: 0,
          backpressure: false,
          reason: 'not_configured'
        }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `WORLD LOOP STATUS: running=${status.running} tickMs=${status.tickMs} lastTickAt=${status.lastTickAt || 0}`,
          `WORLD LOOP LAST_TICK: scheduled=${status.scheduledCount} backpressure=${status.backpressure} reason=${status.reason}`
        ]
      }
    }

    if (parsed.type === 'status') {
      const status = getStatusSnapshot
        ? await getStatusSnapshot({ agents: runtimeAgents })
        : buildDefaultStatus(memoryStore, runtimeAgents, worldLoop)
      const lines = [
        `GOD STATUS: loop_running=${!!status.loopStatus?.running} agents_online=${Number(status.agentsOnline || 0)} last_tick_at=${Number(status.loopStatus?.lastTickAt || 0)}`,
        `GOD STATUS TX: avg=${Number(status.avgTxMs || 0).toFixed(2)}ms p95=${Number(status.p95TxMs || 0).toFixed(2)}ms p99=${Number(status.p99TxMs || 0).toFixed(2)}ms`,
        `GOD STATUS LOCK: lock_wait_p95=${Number(status.lockWaitP95Ms || 0).toFixed(2)}ms lock_wait_p99=${Number(status.lockWaitP99Ms || 0).toFixed(2)}ms`,
        `GOD STATUS MEMORY: memory_bytes=${Number(status.memoryBytes || 0)} heap_mb=${Number(status.heapMb || 0).toFixed(2)}`,
        `GOD STATUS FLAGS: ${(Array.isArray(status.guardrailFlags) && status.guardrailFlags.length) ? status.guardrailFlags.join(' | ') : 'CLEAN'}`
      ]
      return { applied: true, command, audit: false, outputLines: lines }
    }

    if (parsed.type === 'inspect_agent') {
      const snapshot = memoryStore.getSnapshot()
      const match = Object.entries(snapshot.agents || {})
        .find(([name]) => name.toLowerCase() === parsed.name.toLowerCase())
      if (!match) {
        throw new AppError({
          code: 'UNKNOWN_AGENT',
          message: `Unknown agent for inspect: ${parsed.name}`,
          recoverable: true
        })
      }

      const [agentName, agentRecord] = match
      const profile = agentRecord && typeof agentRecord.profile === 'object' ? agentRecord.profile : {}
      const worldIntent = normalizeWorldIntent(profile)
      const trust = profile && Object.prototype.hasOwnProperty.call(profile, 'trust')
        ? Number(profile.trust)
        : null
      const mood = profile && Object.prototype.hasOwnProperty.call(profile, 'mood')
        ? asText(profile.mood, '', 40)
        : ''
      const jobRole = asText(profile?.job?.role, '', 20)
      const jobAssignedAt = asText(profile?.job?.assigned_at, '', 80)
      const jobHomeMarker = asText(profile?.job?.home_marker, '', 80)
      const loopRuntime = worldLoop && typeof worldLoop.getAgentRuntimeState === 'function'
        ? worldLoop.getAgentRuntimeState(agentName)
        : { repetitionCount: 0, selectedIntent: null, selectedTarget: null }
      const selectedIntent = asText(loopRuntime?.selectedIntent, '', 20) || worldIntent.intent
      const selectedTarget = asText(loopRuntime?.selectedTarget, '', 80) || worldIntent.intent_target || '(none)'
      const repetitionCount = Number(loopRuntime?.repetitionCount || 0)
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD INSPECT AGENT: name=${agentName}${trust !== null && Number.isFinite(trust) ? ` trust=${trust}` : ''}${mood ? ` mood=${mood}` : ''}`,
          `GOD INSPECT INTENT: intent=${worldIntent.intent} target=${worldIntent.intent_target || '(none)'} set_at=${worldIntent.intent_set_at || 0}`,
          `GOD INSPECT LOOP: selected_intent=${selectedIntent} selected_target=${selectedTarget} repetition_count=${repetitionCount}`,
          `GOD INSPECT ACTION: last_action=${worldIntent.last_action || '(none)'} last_action_at=${worldIntent.last_action_at || 0}`,
          `GOD INSPECT BUDGETS: minute_bucket=${Number(worldIntent.budgets.minute_bucket || 0)} events_in_min=${Number(worldIntent.budgets.events_in_min || 0)}`,
          `GOD INSPECT FLAGS: manual_override=${worldIntent.manual_override} frozen=${worldIntent.frozen} is_leader=${worldIntent.is_leader}`,
          `GOD INSPECT JOB: role=${jobRole || '(none)'} assigned_at=${jobAssignedAt || 0} home_marker=${jobHomeMarker || '(none)'}`
        ]
      }
    }

    if (parsed.type === 'inspect_world') {
      const status = getStatusSnapshot
        ? await getStatusSnapshot({ agents: runtimeAgents })
        : buildDefaultStatus(memoryStore, runtimeAgents, worldLoop)
      const loopStatus = status.loopStatus || {}
      const runtimeMetrics = status.runtimeMetrics || memoryStore.getRuntimeMetrics()
      const observability = status.observability || getObservabilitySnapshot()
      const avgTxMs = observability.txDurationCount > 0
        ? observability.txDurationTotalMs / observability.txDurationCount
        : 0
      const intentsSelectedTotal = Number(loopStatus.intentsSelectedTotal || 0)
      const fallbackBreaksTotal = Number(loopStatus.fallbackBreaksTotal || 0)
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD INSPECT WORLD LOOP: running=${!!loopStatus.running} tickMs=${Number(loopStatus.tickMs || 0)} lastTickAt=${Number(loopStatus.lastTickAt || 0)} scheduled=${Number(loopStatus.scheduledCount || 0)} backpressure=${!!loopStatus.backpressure} reason=${asText(loopStatus.reason, 'n/a', 80)}`,
          `GOD INSPECT WORLD TICKS: last_ms=${fmt(loopStatus.lastTickDurationMs)} avg_ms=${fmt(loopStatus.avgTickDurationMs)} max_ms=${fmt(loopStatus.maxTickDurationMs)} tick_count=${Number(loopStatus.tickCount || 0)}`,
          `GOD INSPECT WORLD LOOP COUNTERS: intents_selected_total=${intentsSelectedTotal} fallback_breaks_total=${fallbackBreaksTotal}`,
          `GOD INSPECT WORLD TX: avg_ms=${fmt(avgTxMs)} p95_ms=${fmt(observability.txDurationP95Ms)} p99_ms=${fmt(observability.txDurationP99Ms)} max_ms=${fmt(observability.txDurationMaxMs)}`,
          `GOD INSPECT WORLD METRICS: events=${Number(runtimeMetrics.eventsProcessed || 0)} duplicates=${Number(runtimeMetrics.duplicateEventsSkipped || 0)} committed=${Number(runtimeMetrics.transactionsCommitted || 0)} aborted=${Number(runtimeMetrics.transactionsAborted || 0)} lock_retries=${Number(runtimeMetrics.lockRetries || 0)} lock_timeouts=${Number(runtimeMetrics.lockTimeouts || 0)}`
        ]
      }
    }

    if (parsed.type === 'clock_show') {
      const snapshot = memoryStore.getSnapshot()
      const clock = normalizeWorldClock(snapshot.world?.clock)
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD CLOCK: day=${clock.day} phase=${clock.phase} season=${clock.season} updated_at=${clock.updated_at}`
        ]
      }
    }

    if (parsed.type === 'threat_list') {
      const snapshot = memoryStore.getSnapshot()
      const towns = deriveTownsFromMarkers(snapshot.world?.markers || [])
      const threat = normalizeWorldThreat(snapshot.world?.threat)
      const townNames = new Map()
      for (const town of towns) {
        townNames.set(town.townName.toLowerCase(), town.townName)
      }
      for (const townName of Object.keys(threat.byTown || {})) {
        const safeTown = asText(townName, '', 80)
        if (!safeTown) continue
        const key = safeTown.toLowerCase()
        if (!townNames.has(key)) townNames.set(key, safeTown)
      }
      const sortedTownNames = Array.from(townNames.values()).sort((a, b) => a.localeCompare(b))
      if (sortedTownNames.length === 0) {
        return { applied: true, command, audit: false, outputLines: ['GOD THREAT: (none) default=0'] }
      }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD THREAT: count=${sortedTownNames.length}`,
          ...sortedTownNames.map(townName => (
            `GOD THREAT TOWN: town=${townName} level=${Number(threat.byTown[townName] || 0)}`
          ))
        ]
      }
    }

    if (parsed.type === 'threat_show') {
      const snapshot = memoryStore.getSnapshot()
      const towns = deriveTownsFromMarkers(snapshot.world?.markers || [])
      const town = findTownByName(towns, parsed.townName)
      if (!town) return { applied: false, command, reason: 'Unknown town.' }
      const threat = normalizeWorldThreat(snapshot.world?.threat)
      const level = Number(threat.byTown[town.townName] || 0)
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [`GOD THREAT: town=${town.townName} level=${level}`]
      }
    }

    if (parsed.type === 'mood_list') {
      const snapshot = memoryStore.getSnapshot()
      const towns = Array.from(buildTownNameIndex(snapshot.world).values()).sort((a, b) => a.localeCompare(b))
      const moods = normalizeWorldMoods(snapshot.world?.moods)
      const threat = normalizeWorldThreat(snapshot.world?.threat)
      const factions = normalizeWorldStoryFactions(snapshot.world?.factions)
      if (towns.length === 0) {
        return { applied: true, command, audit: false, outputLines: ['GOD MOOD LIST: (none) default=steady'] }
      }
      const lines = [`GOD MOOD LIST: count=${towns.length}`]
      for (const townName of towns) {
        const mood = normalizeTownMood(moods.byTown[townName]) || freshTownMood()
        const label = deriveDominantMoodLabel(mood)
        const level = Number(threat.byTown[townName] || 0)
        const faction = findStoryFactionByTown(factions, townName)
        lines.push(
          `GOD MOOD TOWN: town=${townName} mood=${label} fear=${mood.fear} unrest=${mood.unrest} prosperity=${mood.prosperity} threat=${level} faction=${faction?.name || '-'}`
        )
      }
      return { applied: true, command, audit: false, outputLines: lines }
    }

    if (parsed.type === 'mood_show') {
      const snapshot = memoryStore.getSnapshot()
      const townName = resolveTownName(snapshot.world, parsed.townName)
      if (!townName) return { applied: false, command, reason: 'Unknown town.' }
      const moods = normalizeWorldMoods(snapshot.world?.moods)
      const threat = normalizeWorldThreat(snapshot.world?.threat)
      const factions = normalizeWorldStoryFactions(snapshot.world?.factions)
      const mood = normalizeTownMood(moods.byTown[townName]) || freshTownMood()
      const label = deriveDominantMoodLabel(mood)
      const level = Number(threat.byTown[townName] || 0)
      const faction = findStoryFactionByTown(factions, townName)
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD MOOD: town=${townName} mood=${label} fear=${mood.fear} unrest=${mood.unrest} prosperity=${mood.prosperity} threat=${level} faction=${faction?.name || '-'}`
        ]
      }
    }

    if (parsed.type === 'event_list') {
      const snapshot = memoryStore.getSnapshot()
      const events = normalizeWorldEvents(snapshot.world?.events)
      const clock = normalizeWorldClock(snapshot.world?.clock)
      if (events.active.length === 0) {
        return {
          applied: true,
          command,
          audit: false,
          outputLines: [`GOD EVENT LIST: count=0 seed=${events.seed} index=${events.index} day=${clock.day}`]
        }
      }
      const rows = events.active
        .slice()
        .sort((a, b) => {
          const dayDiff = Number(b.starts_day || 0) - Number(a.starts_day || 0)
          if (dayDiff !== 0) return dayDiff
          const townDiff = asText(a.town, '', 80).localeCompare(asText(b.town, '', 80))
          if (townDiff !== 0) return townDiff
          return asText(a.id, '', 200).localeCompare(asText(b.id, '', 200))
        })
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD EVENT LIST: count=${rows.length} seed=${events.seed} index=${events.index} day=${clock.day}`,
          ...rows.map(event => (
            `GOD EVENT: id=${event.id} type=${event.type} town=${event.town} starts_day=${event.starts_day} ends_day=${event.ends_day} active=${isEventActiveForDay(event, clock.day)} effects=${summarizeEventMods(event.mods)}`
          ))
        ]
      }
    }

    if (parsed.type === 'event_seed') {
      if (!Number.isInteger(parsed.seed)) return { applied: false, command, reason: 'Invalid event seed.' }
      const tx = await memoryStore.transact((memory) => {
        const events = ensureWorldEvents(memory.world)
        const at = now()
        events.seed = parsed.seed
        const message = `EVENT DECK: seed set to ${events.seed}.`
        appendChronicle(memory, {
          id: `${operationId}:chronicle:event_seed`,
          type: 'event',
          msg: message,
          at,
          meta: { seed: events.seed }
        })
        appendNews(memory, {
          id: `${operationId}:news:event_seed`,
          topic: 'world',
          msg: message,
          at,
          meta: { seed: events.seed }
        })
        return { seed: events.seed, index: events.index }
      }, { eventId: `${operationId}:event_seed:${parsed.seed}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD EVENT SEED: seed=${tx.result.seed} index=${tx.result.index}`]
      }
    }

    if (parsed.type === 'event_draw') {
      const snapshot = memoryStore.getSnapshot()
      let requestedTown = null
      if (parsed.townName) {
        const resolvedTown = resolveTownName(snapshot.world, parsed.townName)
        if (!resolvedTown) return { applied: false, command, reason: 'Unknown town.' }
        requestedTown = resolvedTown
      }
      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const at = now()
          const townSlug = (requestedTown || 'auto').toLowerCase()
          const idPrefix = `${operationId}:event_draw:${townSlug}`
          const event = drawAndApplyWorldEvent(memory, {
            operationId,
            idPrefix,
            at,
            requestedTownName: requestedTown
          })
          return event
        }, { eventId: `${operationId}:event_draw:${(requestedTown || 'auto').toLowerCase()}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'UNKNOWN_TOWN') {
          return { applied: false, command, reason: 'Unknown town.' }
        }
        throw err
      }

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [
          `GOD EVENT DRAW: id=${tx.result.id} type=${tx.result.type} town=${tx.result.town} starts_day=${tx.result.starts_day} ends_day=${tx.result.ends_day} effects=${summarizeEventMods(tx.result.mods)}`
        ]
      }
    }

    if (parsed.type === 'event_clear') {
      const snapshot = memoryStore.getSnapshot()
      const events = normalizeWorldEvents(snapshot.world?.events)
      const existing = events.active.find(entry => sameText(entry.id, parsed.eventId, 200))
      if (!existing) return { applied: false, command, reason: 'Unknown event.' }
      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const worldEvents = ensureWorldEvents(memory.world)
          const idx = worldEvents.active.findIndex(entry => sameText(entry?.id, existing.id, 200))
          if (idx < 0) {
            throw new AppError({
              code: 'UNKNOWN_EVENT',
              message: `Unknown event: ${parsed.eventId}`,
              recoverable: true
            })
          }
          const [removedRaw] = worldEvents.active.splice(idx, 1)
          const removed = normalizeWorldEvent(removedRaw) || existing
          const at = now()
          const message = `[${removed.town}] EVENT: cleared ${removed.type} (${removed.id}).`
          appendChronicle(memory, {
            id: `${operationId}:chronicle:event_clear:${removed.id.toLowerCase()}`,
            type: 'event',
            msg: message,
            at,
            town: removed.town,
            meta: {
              event_id: removed.id,
              event_type: removed.type
            }
          })
          appendNews(memory, {
            id: `${operationId}:news:event_clear:${removed.id.toLowerCase()}`,
            topic: 'world',
            msg: message,
            at,
            town: removed.town,
            meta: {
              event_id: removed.id,
              event_type: removed.type
            }
          })
          return removed
        }, { eventId: `${operationId}:event_clear:${existing.id.toLowerCase()}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'UNKNOWN_EVENT') {
          return { applied: false, command, reason: 'Unknown event.' }
        }
        throw err
      }
      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD EVENT CLEAR: id=${tx.result.id} type=${tx.result.type} town=${tx.result.town}`]
      }
    }

    if (parsed.type === 'faction_list') {
      const snapshot = memoryStore.getSnapshot()
      const factions = normalizeWorldStoryFactions(snapshot.world?.factions)
      const lines = [`GOD FACTION LIST: count=${STORY_FACTION_NAMES.length}`]
      for (const factionName of STORY_FACTION_NAMES) {
        const faction = factions[factionName]
        const towns = normalizeStoryTownNames(faction?.towns)
        const rivals = normalizeStoryRivalNames(faction?.rivals, factionName)
        lines.push(
          `GOD FACTION: name=${factionName} towns=${towns.length ? towns.join('|') : '-'} doctrine=${asText(faction?.doctrine, STORY_FACTION_DEFAULTS[factionName].doctrine, 160)} rivals=${rivals.length ? rivals.join('|') : '-'}`
        )
      }
      return { applied: true, command, audit: false, outputLines: lines }
    }

    if (parsed.type === 'rep_show') {
      const snapshot = memoryStore.getSnapshot()
      const agentName = resolveKnownAgentName(snapshot, runtimeAgents, parsed.agentName)
      if (!agentName) return { applied: false, command, reason: 'Unknown agent.' }
      if (parsed.factionName && !STORY_FACTION_NAME_SET.has(parsed.factionName)) {
        return { applied: false, command, reason: 'Unknown faction.' }
      }
      const profile = snapshot.agents?.[agentName]?.profile
      const rep = normalizeAgentRep(profile?.rep)
      if (parsed.factionName) {
        const value = Number(rep[parsed.factionName] || 0)
        return {
          applied: true,
          command,
          audit: false,
          outputLines: [`GOD REP: agent=${agentName} faction=${parsed.factionName} value=${value}`]
        }
      }
      const values = STORY_FACTION_NAMES
        .map(factionName => `${factionName}=${Number(rep[factionName] || 0)}`)
        .join(' ')
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [`GOD REP: agent=${agentName} ${values}`]
      }
    }

    if (parsed.type === 'trait_show') {
      const snapshot = memoryStore.getSnapshot()
      const agentName = resolveKnownAgentName(snapshot, runtimeAgents, parsed.agentName)
      if (!agentName) return { applied: false, command, reason: 'Unknown agent.' }
      const profile = ensureAgentStoryProfile({ ...(snapshot.agents?.[agentName]?.profile || {}) })
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD TRAIT: agent=${agentName} courage=${profile.traits.courage} greed=${profile.traits.greed} faith=${profile.traits.faith}`
        ]
      }
    }

    if (parsed.type === 'title_show') {
      const snapshot = memoryStore.getSnapshot()
      const agentName = resolveKnownAgentName(snapshot, runtimeAgents, parsed.agentName)
      if (!agentName) return { applied: false, command, reason: 'Unknown agent.' }
      const profile = ensureAgentStoryProfile({ ...(snapshot.agents?.[agentName]?.profile || {}) })
      const titles = normalizeAgentTitles(profile.titles)
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD TITLE: agent=${agentName} rumors_completed=${normalizeRumorsCompleted(profile.rumors_completed)} count=${titles.length}`,
          `GOD TITLE LIST: ${titles.length ? titles.join('|') : '(none)'}`
        ]
      }
    }

    if (parsed.type === 'rumor_list') {
      const snapshot = memoryStore.getSnapshot()
      let townFilter = null
      if (parsed.townName) {
        townFilter = resolveTownName(snapshot.world, parsed.townName)
        if (!townFilter) return { applied: false, command, reason: 'Unknown town.' }
      }
      const limit = Math.max(1, Math.min(200, Number(parsed.limit || 10)))
      const rumors = normalizeWorldRumors(snapshot.world?.rumors)
        .filter(rumor => !townFilter || sameText(rumor.town, townFilter, 80))
        .sort((a, b) => {
          const diff = Number(b.created_at || 0) - Number(a.created_at || 0)
          if (diff !== 0) return diff
          return a.id.localeCompare(b.id)
        })
      const rows = rumors.slice(0, limit)
      if (rows.length === 0) {
        return {
          applied: true,
          command,
          audit: false,
          outputLines: [`GOD RUMOR LIST: town=${townFilter || '-'} (none)`]
        }
      }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD RUMOR LIST: town=${townFilter || '-'} count=${rows.length} total=${rumors.length}`,
          ...rows.map(rumor => (
            `GOD RUMOR: id=${rumor.id} town=${rumor.town} kind=${rumor.kind} severity=${rumor.severity} starts_day=${rumor.starts_day} expires_day=${rumor.expires_day} text=${rumor.text}`
          ))
        ]
      }
    }

    if (parsed.type === 'rumor_show') {
      const snapshot = memoryStore.getSnapshot()
      const rumor = findRumorById(snapshot.world?.rumors || [], parsed.rumorId)
      if (!rumor) return { applied: false, command, reason: 'Unknown rumor.' }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD RUMOR SHOW: id=${rumor.id} town=${rumor.town} kind=${rumor.kind} severity=${rumor.severity}`,
          `GOD RUMOR WINDOW: starts_day=${rumor.starts_day} expires_day=${rumor.expires_day} created_at=${rumor.created_at}`,
          `GOD RUMOR LINKS: spawned_by_event_id=${rumor.spawned_by_event_id || '-'} resolved_by_quest_id=${rumor.resolved_by_quest_id || '-'}`,
          `GOD RUMOR TEXT: ${rumor.text}`
        ]
      }
    }

    if (parsed.type === 'decision_list') {
      const snapshot = memoryStore.getSnapshot()
      let townFilter = null
      if (parsed.townName) {
        townFilter = resolveTownName(snapshot.world, parsed.townName)
        if (!townFilter) return { applied: false, command, reason: 'Unknown town.' }
      }
      const decisions = normalizeWorldDecisions(snapshot.world?.decisions)
        .filter(decision => !townFilter || sameText(decision.town, townFilter, 80))
        .sort((a, b) => {
          const diff = Number(b.created_at || 0) - Number(a.created_at || 0)
          if (diff !== 0) return diff
          return a.id.localeCompare(b.id)
        })
      if (decisions.length === 0) {
        return {
          applied: true,
          command,
          audit: false,
          outputLines: [
            `GOD DECISION DEPRECATED: ${DECISION_DEPRECATION_NOTE}`,
            `GOD DECISION LIST: town=${townFilter || '-'} (none)`
          ]
        }
      }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD DECISION DEPRECATED: ${DECISION_DEPRECATION_NOTE}`,
          `GOD DECISION LIST: town=${townFilter || '-'} count=${decisions.length}`,
          ...decisions.map(decision => (
            `GOD DECISION: id=${decision.id} town=${decision.town} event_id=${decision.event_id} event_type=${decision.event_type} state=${decision.state} chosen_key=${decision.chosen_key || '-'} expires_day=${decision.expires_day}`
          ))
        ]
      }
    }

    if (parsed.type === 'decision_show') {
      const snapshot = memoryStore.getSnapshot()
      const decision = findDecisionById(snapshot.world?.decisions || [], parsed.decisionId)
      if (!decision) return { applied: false, command, reason: 'Unknown decision.' }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD DECISION DEPRECATED: ${DECISION_DEPRECATION_NOTE}`,
          `GOD DECISION SHOW: id=${decision.id} town=${decision.town} event_id=${decision.event_id} event_type=${decision.event_type} state=${decision.state} chosen_key=${decision.chosen_key || '-'}`,
          `GOD DECISION PROMPT: ${decision.prompt}`,
          ...decision.options.map(option => (
            `GOD DECISION OPTION: key=${option.key} label=${option.label}`
          ))
        ]
      }
    }

    if (parsed.type === 'clock_season') {
      if (!CLOCK_SEASONS.has(parsed.season)) return { applied: false, command, reason: 'Invalid season.' }

      const tx = await memoryStore.transact((memory) => {
        const at = now()
        const clock = ensureWorldClock(memory.world)
        clock.season = parsed.season
        clock.updated_at = new Date(at).toISOString()
        const message = `Season shifts to ${clock.season}. The world feels different.`
        appendChronicle(memory, {
          id: `${operationId}:chronicle:clock_season:${clock.season}`,
          type: 'clock',
          msg: message,
          at,
          meta: { season: clock.season }
        })
        appendNews(memory, {
          id: `${operationId}:news:clock_season:${clock.season}`,
          topic: 'world',
          msg: message,
          at,
          meta: { season: clock.season }
        })
        return { day: clock.day, phase: clock.phase, season: clock.season }
      }, { eventId: `${operationId}:clock_season:${parsed.season}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD CLOCK SEASON: day=${tx.result.day} phase=${tx.result.phase} season=${tx.result.season}`]
      }
    }

    if (parsed.type === 'clock_advance') {
      const ticks = Number(parsed.ticks)
      if (!Number.isInteger(ticks) || ticks < 1 || ticks > 1000) {
        return { applied: false, command, reason: 'Invalid ticks.' }
      }

      const tx = await memoryStore.transact((memory) => {
        let clock = ensureWorldClock(memory.world)
        const threat = ensureWorldThreat(memory.world)
        ensureWorldMoods(memory.world)
        ensureWorldEvents(memory.world)
        const towns = deriveTownsFromMarkers(memory.world?.markers || [])
        for (let tickIdx = 0; tickIdx < ticks; tickIdx += 1) {
          clock = ensureWorldClock(memory.world)
          const rates = SEASON_THREAT_RATES[clock.season] || SEASON_THREAT_RATES.dawn
          const nextPhase = clock.phase === 'day' ? 'night' : 'day'
          clock.phase = nextPhase
          if (nextPhase === 'day') clock.day += 1
          const at = now()
          expireRumorsForDay(memory, clock.day)
          expireDecisionsForDay(memory, clock.day)

          appendChronicle(memory, {
            id: `${operationId}:chronicle:clock_advance:tick:${tickIdx}`,
            type: 'clock',
            msg: `CLOCK: day=${clock.day} phase=${clock.phase} season=${clock.season}`,
            at,
            meta: {
              day: clock.day,
              phase: clock.phase,
              season: clock.season
            }
          })
          appendNews(memory, {
            id: `${operationId}:news:clock_advance:tick:${tickIdx}`,
            topic: 'world',
            msg: nextPhase === 'night'
              ? 'Lanterns flare as caravans pull off dark roads.'
              : 'Dawn opens the stalls and caravan bells return.',
            at,
            meta: {
              day: clock.day,
              phase: clock.phase,
              season: clock.season
            }
          })

          for (const town of towns) {
            const townName = town.townName
            const current = Number(threat.byTown[townName] || 0)
            const nextLevel = nextPhase === 'night'
              ? clamp(Math.trunc(current + rates.nightRise), 0, 100)
              : clamp(Math.trunc(current - rates.dayFall), 0, 100)
            threat.byTown[townName] = nextLevel
            const message = nextPhase === 'night'
              ? `[${townName}] Night routes darken. Route risk climbs to ${nextLevel}.`
              : `[${townName}] Dawn trade resumes. Route risk eases to ${nextLevel}.`
            const townKey = townName.toLowerCase()
            appendChronicle(memory, {
              id: `${operationId}:chronicle:clock_advance:${tickIdx}:${townKey}`,
              type: 'threat',
              msg: message,
              at,
              town: townName,
              meta: {
                phase: nextPhase,
                level: nextLevel
              }
            })
            appendNews(memory, {
              id: `${operationId}:news:clock_advance:${tickIdx}:${townKey}`,
              topic: 'world',
              msg: message,
              at,
              town: townName,
              meta: {
                phase: nextPhase,
                level: nextLevel
              }
            })
            applyTownMoodDelta(memory, {
              townName,
              delta: { fear: nextPhase === 'night' ? 3 : -2 },
              at,
              idPrefix: `${operationId}:clock_advance:${tickIdx}:${townKey}`,
              reason: 'clock_advance'
            })
          }
          if (nextPhase === 'day') {
            generateDailyContracts(memory, {
              operationId,
              idPrefix: `${operationId}:clock_advance:${tickIdx}:contracts`,
              day: clock.day,
              at
            })
            advanceNetherToDay(memory, {
              targetDay: clock.day,
              at,
              idPrefix: `${operationId}:clock_advance:${tickIdx}:nether`
            })
          }
          if (nextPhase === 'night') {
            emitNightCaravanTrouble(memory, {
              operationId,
              idPrefix: `${operationId}:clock_advance:${tickIdx}:night_warning`,
              tickIdx,
              at,
              towns
            })
            drawAndApplyWorldEvent(memory, {
              operationId,
              idPrefix: `${operationId}:clock_advance:${tickIdx}:nightfall`,
              at
            })
            clock = ensureWorldClock(memory.world)
          }
        }
        clock = ensureWorldClock(memory.world)
        clock.updated_at = new Date(now()).toISOString()
        return {
          day: clock.day,
          phase: clock.phase,
          season: clock.season
        }
      }, { eventId: `${operationId}:clock_advance:${ticks}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD CLOCK ADVANCE: ticks=${ticks} day=${tx.result.day} phase=${tx.result.phase} season=${tx.result.season}`]
      }
    }

    if (parsed.type === 'threat_set') {
      if (parsed.value === null) return { applied: false, command, reason: 'Invalid threat value.' }
      const snapshot = memoryStore.getSnapshot()
      const towns = deriveTownsFromMarkers(snapshot.world?.markers || [])
      const town = findTownByName(towns, parsed.townName)
      if (!town) return { applied: false, command, reason: 'Unknown town.' }
      const level = clamp(Math.trunc(parsed.value), 0, 100)

      const tx = await memoryStore.transact((memory) => {
        const threat = ensureWorldThreat(memory.world)
        const at = now()
        threat.byTown[town.townName] = level
        const message = `[${town.townName}] Threat set to ${level}.`
        appendChronicle(memory, {
          id: `${operationId}:chronicle:threat_set:${town.townName.toLowerCase()}`,
          type: 'threat',
          msg: message,
          at,
          town: town.townName,
          meta: { level }
        })
        appendNews(memory, {
          id: `${operationId}:news:threat_set:${town.townName.toLowerCase()}`,
          topic: 'world',
          msg: message,
          at,
          town: town.townName,
          meta: { level }
        })
        return { townName: town.townName, level }
      }, { eventId: `${operationId}:threat_set:${town.townName.toLowerCase()}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD THREAT SET: town=${tx.result.townName} level=${tx.result.level}`]
      }
    }

    if (parsed.type === 'faction_set') {
      if (!STORY_FACTION_NAME_SET.has(parsed.factionName)) return { applied: false, command, reason: 'Unknown faction.' }
      const snapshot = memoryStore.getSnapshot()
      const towns = deriveTownsFromMarkers(snapshot.world?.markers || [])
      const town = findTownByName(towns, parsed.townName)
      if (!town) return { applied: false, command, reason: 'Unknown town.' }

      const tx = await memoryStore.transact((memory) => {
        const factions = ensureWorldStoryFactions(memory.world)
        for (const factionName of STORY_FACTION_NAMES) {
          const faction = factions[factionName]
          const townsForFaction = normalizeStoryTownNames(faction?.towns)
            .filter(item => item.toLowerCase() !== town.townName.toLowerCase())
          faction.towns = townsForFaction
        }
        const targetFaction = factions[parsed.factionName]
        const townsForTarget = normalizeStoryTownNames(targetFaction.towns)
        if (!townsForTarget.some(item => item.toLowerCase() === town.townName.toLowerCase())) {
          townsForTarget.push(town.townName)
          townsForTarget.sort((a, b) => a.localeCompare(b))
        }
        targetFaction.towns = townsForTarget

        const at = now()
        const message = `[${town.townName}] The banners of ${parsed.factionName} fly over the gates.`
        appendChronicle(memory, {
          id: `${operationId}:chronicle:faction_set:${town.townName.toLowerCase()}`,
          type: 'faction',
          msg: message,
          at,
          town: town.townName,
          meta: {
            faction: parsed.factionName
          }
        })
        appendNews(memory, {
          id: `${operationId}:news:faction_set:${town.townName.toLowerCase()}`,
          topic: 'faction',
          msg: message,
          at,
          town: town.townName,
          meta: {
            faction: parsed.factionName
          }
        })
        return { townName: town.townName, factionName: parsed.factionName }
      }, { eventId: `${operationId}:faction_set:${town.townName.toLowerCase()}:${parsed.factionName}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD FACTION SET: town=${tx.result.townName} faction=${tx.result.factionName}`]
      }
    }

    if (parsed.type === 'rep_add') {
      if (!STORY_FACTION_NAME_SET.has(parsed.factionName)) return { applied: false, command, reason: 'Unknown faction.' }
      if (!Number.isInteger(parsed.delta)) return { applied: false, command, reason: 'Invalid delta.' }
      const snapshot = memoryStore.getSnapshot()
      const agentName = resolveKnownAgentName(snapshot, runtimeAgents, parsed.agentName)
      if (!agentName) return { applied: false, command, reason: 'Unknown agent.' }

      const tx = await memoryStore.transact((memory) => {
        const profile = ensureAgentStoryProfile(ensureAgentProfile(memory, agentName))
        const rep = normalizeAgentRep(profile.rep)
        const current = Number(rep[parsed.factionName] || 0)
        const next = current + parsed.delta
        rep[parsed.factionName] = next
        profile.rep = rep
        const at = now()
        applyRepThresholdTitleAwards(memory, agentName, at, `${operationId}:rep_add_titles`, null)
        const message = parsed.delta >= 0
          ? `${agentName} gains favor with ${parsed.factionName} (${next}).`
          : `${agentName} loses favor with ${parsed.factionName} (${next}).`
        appendChronicle(memory, {
          id: `${operationId}:chronicle:rep_add:${agentName.toLowerCase()}:${parsed.factionName}`,
          type: 'rep',
          msg: message,
          at,
          meta: {
            agent: agentName,
            faction: parsed.factionName,
            delta: parsed.delta,
            rep: next
          }
        })
        appendNews(memory, {
          id: `${operationId}:news:rep_add:${agentName.toLowerCase()}:${parsed.factionName}`,
          topic: 'faction',
          msg: message,
          at,
          meta: {
            agent: agentName,
            faction: parsed.factionName,
            delta: parsed.delta,
            rep: next
          }
        })
        return { agentName, factionName: parsed.factionName, delta: parsed.delta, rep: next }
      }, { eventId: `${operationId}:rep_add:${agentName.toLowerCase()}:${parsed.factionName}:${parsed.delta}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD REP ADD: agent=${tx.result.agentName} faction=${tx.result.factionName} delta=${tx.result.delta} rep=${tx.result.rep}`]
      }
    }

    if (parsed.type === 'trait_set') {
      if (!TRAIT_NAME_SET.has(parsed.traitName)) return { applied: false, command, reason: 'Invalid trait.' }
      if (!Number.isInteger(parsed.value) || parsed.value < 0 || parsed.value > 3) {
        return { applied: false, command, reason: 'Invalid trait value.' }
      }
      const snapshot = memoryStore.getSnapshot()
      const agentName = resolveKnownAgentName(snapshot, runtimeAgents, parsed.agentName)
      if (!agentName) return { applied: false, command, reason: 'Unknown agent.' }

      const tx = await memoryStore.transact((memory) => {
        const profile = ensureAgentStoryProfile(ensureAgentProfile(memory, agentName))
        const traits = normalizeAgentTraits(profile.traits)
        traits[parsed.traitName] = parsed.value
        profile.traits = traits
        const at = now()
        appendChronicle(memory, {
          id: `${operationId}:chronicle:trait_set:${agentName.toLowerCase()}:${parsed.traitName}`,
          type: 'trait',
          msg: `TRAIT: ${agentName} ${parsed.traitName}=${parsed.value}`,
          at,
          meta: {
            agent: agentName,
            trait: parsed.traitName,
            value: parsed.value
          }
        })
        appendNews(memory, {
          id: `${operationId}:news:trait_set:${agentName.toLowerCase()}:${parsed.traitName}`,
          topic: 'trait',
          msg: `TRAIT: ${agentName} ${parsed.traitName}=${parsed.value}`,
          at,
          meta: {
            agent: agentName,
            trait: parsed.traitName,
            value: parsed.value
          }
        })
        return { agentName, traitName: parsed.traitName, value: parsed.value }
      }, { eventId: `${operationId}:trait_set:${agentName.toLowerCase()}:${parsed.traitName}:${parsed.value}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD TRAIT SET: agent=${tx.result.agentName} ${tx.result.traitName}=${tx.result.value}`]
      }
    }

    if (parsed.type === 'title_grant') {
      const snapshot = memoryStore.getSnapshot()
      const agentName = resolveKnownAgentName(snapshot, runtimeAgents, parsed.agentName)
      if (!agentName) return { applied: false, command, reason: 'Unknown agent.' }
      const title = asText(parsed.title, '', MAX_AGENT_TITLE_LEN)
      if (!title) return { applied: false, command, reason: 'Invalid title.' }
      const existingTitles = normalizeAgentTitles(snapshot.agents?.[agentName]?.profile?.titles)
      if (existingTitles.some(item => sameText(item, title, MAX_AGENT_TITLE_LEN))) {
        return { applied: false, command, reason: 'Title already granted.' }
      }

      const tx = await memoryStore.transact((memory) => {
        const granted = grantAgentTitleIfMissing(memory, agentName, title, now(), `${operationId}:title_grant`, null)
        return { agentName, title, granted }
      }, { eventId: `${operationId}:title_grant:${agentName.toLowerCase()}:${title.toLowerCase()}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      if (!tx.result.granted) return { applied: false, command, reason: 'Title already granted.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD TITLE GRANT: agent=${tx.result.agentName} title=${tx.result.title}`]
      }
    }

    if (parsed.type === 'title_revoke') {
      const snapshot = memoryStore.getSnapshot()
      const agentName = resolveKnownAgentName(snapshot, runtimeAgents, parsed.agentName)
      if (!agentName) return { applied: false, command, reason: 'Unknown agent.' }
      const title = asText(parsed.title, '', MAX_AGENT_TITLE_LEN)
      if (!title) return { applied: false, command, reason: 'Invalid title.' }
      const existingTitles = normalizeAgentTitles(snapshot.agents?.[agentName]?.profile?.titles)
      if (!existingTitles.some(item => sameText(item, title, MAX_AGENT_TITLE_LEN))) {
        return { applied: false, command, reason: 'Unknown title.' }
      }

      const tx = await memoryStore.transact((memory) => {
        const profile = ensureAgentStoryProfile(ensureAgentProfile(memory, agentName))
        const nextTitles = normalizeAgentTitles(profile.titles)
          .filter(item => !sameText(item, title, MAX_AGENT_TITLE_LEN))
        profile.titles = nextTitles
        const at = now()
        const message = `${agentName} loses the title "${title}".`
        appendChronicle(memory, {
          id: `${operationId}:chronicle:title_revoke:${agentName.toLowerCase()}:${title.toLowerCase()}`,
          type: 'title',
          msg: message,
          at,
          meta: {
            agent: agentName,
            title
          }
        })
        appendNews(memory, {
          id: `${operationId}:news:title_revoke:${agentName.toLowerCase()}:${title.toLowerCase()}`,
          topic: 'title',
          msg: message,
          at,
          meta: {
            agent: agentName,
            title
          }
        })
        return { agentName, title }
      }, { eventId: `${operationId}:title_revoke:${agentName.toLowerCase()}:${title.toLowerCase()}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD TITLE REVOKE: agent=${tx.result.agentName} title=${tx.result.title}`]
      }
    }

    if (parsed.type === 'rumor_spawn') {
      const snapshot = memoryStore.getSnapshot()
      const townName = resolveTownName(snapshot.world, parsed.townName)
      if (!townName) return { applied: false, command, reason: 'Unknown town.' }
      if (!RUMOR_KIND_SET.has(parsed.kind)) return { applied: false, command, reason: 'Invalid rumor kind.' }
      if (!Number.isInteger(parsed.severity) || parsed.severity < 1 || parsed.severity > 3) {
        return { applied: false, command, reason: 'Invalid rumor severity.' }
      }
      if (!renderRumorTemplate({
        kind: parsed.kind,
        templateKey: parsed.templateKey,
        townName,
        phase: normalizeWorldClock(snapshot.world?.clock).phase
      })) {
        return { applied: false, command, reason: 'Unknown rumor template.' }
      }
      if (parsed.expiresInDays !== null && (!Number.isInteger(parsed.expiresInDays) || parsed.expiresInDays < 0)) {
        return { applied: false, command, reason: 'Invalid rumor expiry.' }
      }

      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const rumor = spawnWorldRumor(memory, {
            townName,
            kind: parsed.kind,
            severity: parsed.severity,
            templateKey: parsed.templateKey,
            expiresInDays: parsed.expiresInDays,
            at: now(),
            idPrefix: `${operationId}:rumor_spawn`
          })
          return rumor
        }, { eventId: `${operationId}:rumor_spawn:${townName.toLowerCase()}:${parsed.kind}:${parsed.templateKey}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'UNKNOWN_TOWN') return { applied: false, command, reason: 'Unknown town.' }
        if (err instanceof AppError && err.code === 'INVALID_RUMOR_KIND') return { applied: false, command, reason: 'Invalid rumor kind.' }
        if (err instanceof AppError && err.code === 'INVALID_RUMOR_SEVERITY') return { applied: false, command, reason: 'Invalid rumor severity.' }
        if (err instanceof AppError && err.code === 'UNKNOWN_RUMOR_TEMPLATE') return { applied: false, command, reason: 'Unknown rumor template.' }
        throw err
      }
      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD RUMOR SPAWN: id=${tx.result.id} town=${tx.result.town} kind=${tx.result.kind} severity=${tx.result.severity} expires_day=${tx.result.expires_day}`]
      }
    }

    if (parsed.type === 'rumor_clear') {
      const snapshot = memoryStore.getSnapshot()
      const rumor = findRumorById(snapshot.world?.rumors || [], parsed.rumorId)
      if (!rumor) return { applied: false, command, reason: 'Unknown rumor.' }

      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const rumors = ensureWorldRumors(memory.world)
          const idx = rumors.findIndex(entry => sameText(entry?.id, rumor.id, 200))
          if (idx < 0) {
            throw new AppError({
              code: 'UNKNOWN_RUMOR',
              message: `Unknown rumor: ${parsed.rumorId}`,
              recoverable: true
            })
          }
          const [removedRaw] = rumors.splice(idx, 1)
          const removed = normalizeRumor(removedRaw) || rumor
          const at = now()
          appendChronicle(memory, {
            id: `${operationId}:chronicle:rumor_clear:${removed.id.toLowerCase()}`,
            type: 'rumor_clear',
            msg: `RUMOR: cleared ${removed.id}`,
            at,
            town: removed.town,
            meta: {
              rumor_id: removed.id
            }
          })
          appendNews(memory, {
            id: `${operationId}:news:rumor_clear:${removed.id.toLowerCase()}`,
            topic: 'rumor',
            msg: `RUMOR: cleared ${removed.id}`,
            at,
            town: removed.town,
            meta: {
              rumor_id: removed.id
            }
          })
          return removed
        }, { eventId: `${operationId}:rumor_clear:${rumor.id.toLowerCase()}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'UNKNOWN_RUMOR') return { applied: false, command, reason: 'Unknown rumor.' }
        throw err
      }
      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD RUMOR CLEAR: id=${tx.result.id}`]
      }
    }

    if (parsed.type === 'rumor_resolve') {
      const snapshot = memoryStore.getSnapshot()
      const rumor = findRumorById(snapshot.world?.rumors || [], parsed.rumorId)
      if (!rumor) return { applied: false, command, reason: 'Unknown rumor.' }
      const quest = findQuestById(snapshot.world?.quests || [], parsed.questId)
      if (!quest) return { applied: false, command, reason: 'Unknown quest.' }
      if (sameText(rumor.resolved_by_quest_id, quest.id, 200)) {
        return { applied: false, command, reason: 'Rumor already resolved by this quest.' }
      }

      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const rumors = ensureWorldRumors(memory.world)
          const quests = ensureWorldQuests(memory.world)
          const rumorIdx = rumors.findIndex(entry => sameText(entry?.id, rumor.id, 200))
          const questRecord = findQuestById(quests, quest.id)
          if (rumorIdx < 0) {
            throw new AppError({
              code: 'UNKNOWN_RUMOR',
              message: `Unknown rumor: ${parsed.rumorId}`,
              recoverable: true
            })
          }
          if (!questRecord) {
            throw new AppError({
              code: 'UNKNOWN_QUEST',
              message: `Unknown quest: ${parsed.questId}`,
              recoverable: true
            })
          }
          const current = normalizeRumor(rumors[rumorIdx]) || rumor
          rumors[rumorIdx] = {
            ...current,
            resolved_by_quest_id: questRecord.id
          }
          const at = now()
          const message = `RUMOR: ${current.id} resolved by quest ${questRecord.id}`
          appendChronicle(memory, {
            id: `${operationId}:chronicle:rumor_resolve:${current.id.toLowerCase()}`,
            type: 'rumor_resolve',
            msg: message,
            at,
            town: current.town,
            meta: {
              rumor_id: current.id,
              quest_id: questRecord.id
            }
          })
          appendNews(memory, {
            id: `${operationId}:news:rumor_resolve:${current.id.toLowerCase()}`,
            topic: 'rumor',
            msg: message,
            at,
            town: current.town,
            meta: {
              rumor_id: current.id,
              quest_id: questRecord.id
            }
          })
          return {
            rumorId: current.id,
            questId: questRecord.id
          }
        }, { eventId: `${operationId}:rumor_resolve:${rumor.id.toLowerCase()}:${quest.id.toLowerCase()}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'UNKNOWN_RUMOR') return { applied: false, command, reason: 'Unknown rumor.' }
        if (err instanceof AppError && err.code === 'UNKNOWN_QUEST') return { applied: false, command, reason: 'Unknown quest.' }
        throw err
      }

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD RUMOR RESOLVE: rumor_id=${tx.result.rumorId} quest_id=${tx.result.questId}`]
      }
    }

    if (parsed.type === 'rumor_quest') {
      const snapshot = memoryStore.getSnapshot()
      const rumor = findRumorById(snapshot.world?.rumors || [], parsed.rumorId)
      if (!rumor) return { applied: false, command, reason: 'Unknown rumor.' }
      const clock = normalizeWorldClock(snapshot.world?.clock)
      if (Number(rumor.expires_day || 0) < clock.day) return { applied: false, command, reason: 'Rumor expired.' }
      const existingSideQuest = normalizeWorldQuests(snapshot.world?.quests)
        .find(item => item.type === 'rumor_task' && sameText(item.rumor_id, rumor.id, 200) && item.state !== 'cancelled')
      if (existingSideQuest) return { applied: false, command, reason: 'Rumor already has side quest.' }

      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const rumorCurrent = findRumorById(memory.world?.rumors || [], rumor.id)
          if (!rumorCurrent) {
            throw new AppError({
              code: 'UNKNOWN_RUMOR',
              message: `Unknown rumor: ${parsed.rumorId}`,
              recoverable: true
            })
          }
          const day = ensureWorldClock(memory.world).day
          if (rumorCurrent.expires_day < day) {
            throw new AppError({
              code: 'RUMOR_EXPIRED',
              message: `Rumor expired: ${rumorCurrent.id}`,
              recoverable: true
            })
          }
          const existing = normalizeWorldQuests(memory.world?.quests)
            .find(item => item.type === 'rumor_task' && sameText(item.rumor_id, rumorCurrent.id, 200) && item.state !== 'cancelled')
          if (existing) {
            throw new AppError({
              code: 'RUMOR_QUEST_EXISTS',
              message: `Rumor side quest already exists: ${rumorCurrent.id}`,
              recoverable: true
            })
          }
          return createRumorSideQuest(memory, rumorCurrent, { operationId, at: now() })
        }, { eventId: `${operationId}:rumor_quest:${rumor.id.toLowerCase()}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'UNKNOWN_RUMOR') return { applied: false, command, reason: 'Unknown rumor.' }
        if (err instanceof AppError && err.code === 'RUMOR_EXPIRED') return { applied: false, command, reason: 'Rumor expired.' }
        if (err instanceof AppError && err.code === 'RUMOR_QUEST_EXISTS') return { applied: false, command, reason: 'Rumor already has side quest.' }
        throw err
      }
      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD RUMOR QUEST: rumor_id=${tx.result.rumor_id} quest_id=${tx.result.id} reward=${tx.result.reward}`]
      }
    }

    if (parsed.type === 'decision_choose') {
      const snapshot = memoryStore.getSnapshot()
      const existing = findDecisionById(snapshot.world?.decisions || [], parsed.decisionId)
      if (!existing) return { applied: false, command, reason: 'Unknown decision.' }
      if (existing.state !== 'open') return { applied: false, command, reason: 'Decision is not open.' }
      const option = existing.options.find(item => sameText(item.key, parsed.optionKey, 40))
      if (!option) return { applied: false, command, reason: 'Unknown option.' }

      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const decisions = ensureWorldDecisions(memory.world)
          const idx = decisions.findIndex(item => sameText(item?.id, existing.id, 200))
          if (idx < 0) {
            throw new AppError({
              code: 'UNKNOWN_DECISION',
              message: `Unknown decision: ${parsed.decisionId}`,
              recoverable: true
            })
          }
          const decision = normalizeDecision(decisions[idx])
          if (!decision || decision.state !== 'open') {
            throw new AppError({
              code: 'DECISION_NOT_OPEN',
              message: `Decision is not open: ${parsed.decisionId}`,
              recoverable: true
            })
          }
          const selectedOption = decision.options.find(item => sameText(item.key, parsed.optionKey, 40))
          if (!selectedOption) {
            throw new AppError({
              code: 'UNKNOWN_OPTION',
              message: `Unknown option: ${parsed.optionKey}`,
              recoverable: true
            })
          }
          const at = now()
          decision.state = 'chosen'
          decision.chosen_key = selectedOption.key
          decisions[idx] = decision
          const effectResult = applyDecisionChoiceEffects(memory, decision, selectedOption, {
            operationId: `${operationId}:decision_choose:${decision.id.toLowerCase()}`,
            at
          })
          const message = `[${decision.town}] LEGACY DECISION chose ${selectedOption.key} for ${decision.event_type}.`
          appendChronicle(memory, {
            id: `${operationId}:chronicle:decision_choose:${decision.id.toLowerCase()}`,
            type: 'decision_choose',
            msg: message,
            at,
            town: decision.town,
            meta: {
              decision_id: decision.id,
              option_key: selectedOption.key
            }
          })
          appendNews(memory, {
            id: `${operationId}:news:decision_choose:${decision.id.toLowerCase()}`,
            topic: 'world',
            msg: message,
            at,
            town: decision.town,
            meta: {
              decision_id: decision.id,
              option_key: selectedOption.key
            }
          })
          return {
            id: decision.id,
            town: decision.town,
            optionKey: selectedOption.key,
            effectSummary: effectResult?.summary || []
          }
        }, { eventId: `${operationId}:decision_choose:${existing.id.toLowerCase()}:${parsed.optionKey}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'UNKNOWN_DECISION') return { applied: false, command, reason: 'Unknown decision.' }
        if (err instanceof AppError && err.code === 'DECISION_NOT_OPEN') return { applied: false, command, reason: 'Decision is not open.' }
        if (err instanceof AppError && err.code === 'UNKNOWN_OPTION') return { applied: false, command, reason: 'Unknown option.' }
        throw err
      }
      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD DECISION CHOOSE: id=${tx.result.id} town=${tx.result.town} option=${tx.result.optionKey} effects=${tx.result.effectSummary.length ? tx.result.effectSummary.join('|') : '-'}`]
      }
    }

    if (parsed.type === 'decision_expire') {
      const snapshot = memoryStore.getSnapshot()
      const existing = findDecisionById(snapshot.world?.decisions || [], parsed.decisionId)
      if (!existing) return { applied: false, command, reason: 'Unknown decision.' }
      if (existing.state === 'expired') return { applied: false, command, reason: 'Decision already expired.' }

      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const decisions = ensureWorldDecisions(memory.world)
          const idx = decisions.findIndex(item => sameText(item?.id, existing.id, 200))
          if (idx < 0) {
            throw new AppError({
              code: 'UNKNOWN_DECISION',
              message: `Unknown decision: ${parsed.decisionId}`,
              recoverable: true
            })
          }
          const decision = normalizeDecision(decisions[idx])
          if (!decision) {
            throw new AppError({
              code: 'UNKNOWN_DECISION',
              message: `Unknown decision: ${parsed.decisionId}`,
              recoverable: true
            })
          }
          decision.state = 'expired'
          delete decision.chosen_key
          decisions[idx] = decision
          const at = now()
          appendChronicle(memory, {
            id: `${operationId}:chronicle:decision_expire:${decision.id.toLowerCase()}`,
            type: 'decision_expire',
            msg: `[${decision.town}] LEGACY decision expired: ${decision.id}`,
            at,
            town: decision.town,
            meta: {
              decision_id: decision.id
            }
          })
          appendNews(memory, {
            id: `${operationId}:news:decision_expire:${decision.id.toLowerCase()}`,
            topic: 'world',
            msg: `[${decision.town}] LEGACY decision expired: ${decision.id}`,
            at,
            town: decision.town,
            meta: {
              decision_id: decision.id
            }
          })
          return decision
        }, { eventId: `${operationId}:decision_expire:${existing.id.toLowerCase()}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'UNKNOWN_DECISION') return { applied: false, command, reason: 'Unknown decision.' }
        throw err
      }
      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD DECISION EXPIRE: id=${tx.result.id} town=${tx.result.town}`]
      }
    }

    if (parsed.type === 'town_list') {
      const snapshot = memoryStore.getSnapshot()
      const towns = deriveTownsFromMarkers(snapshot.world?.markers || [])
      const markets = normalizeWorldMarkets(snapshot.world?.markets)
      if (towns.length === 0) {
        return { applied: true, command, audit: false, outputLines: ['GOD TOWN LIST: (none)'] }
      }
      const lines = [`GOD TOWN LIST: count=${towns.length}`]
      for (const town of towns) {
        let population = 0
        for (const record of Object.values(snapshot.agents || {})) {
          const homeMarker = asText(record?.profile?.job?.home_marker, '', 80)
          if (sameText(homeMarker, town.marker.name, 80)) population += 1
        }
        const marketCount = markets
          .filter(market => sameText(market.marker, town.marker.name, 80))
          .length
        lines.push(
          `GOD TOWN: name=${town.townName} marker=${town.marker.name} x=${town.marker.x} y=${town.marker.y} z=${town.marker.z} tag=${town.marker.tag || '(none)'} population=${population} markets=${marketCount}`
        )
      }
      return { applied: true, command, audit: false, outputLines: lines }
    }

    if (parsed.type === 'town_board') {
      const snapshot = memoryStore.getSnapshot()
      const towns = deriveTownsFromMarkers(snapshot.world?.markers || [])
      const town = findTownByName(towns, parsed.townName)
      if (!town) return { applied: false, command, reason: 'Unknown town.' }

      const economy = normalizeWorldEconomy(snapshot.world?.economy)
      const markets = normalizeWorldMarkets(snapshot.world?.markets)
      const chronicle = normalizeWorldChronicle(snapshot.world?.chronicle)
      const clock = normalizeWorldClock(snapshot.world?.clock)
      const threat = normalizeWorldThreat(snapshot.world?.threat)
      const moods = normalizeWorldMoods(snapshot.world?.moods)
      const factions = normalizeWorldStoryFactions(snapshot.world?.factions)
      const limit = Math.max(1, Math.min(200, Number(parsed.limit || 10)))
      const townThreatLevel = Number(threat.byTown[town.townName] || 0)
      const townMood = normalizeTownMood(moods.byTown[town.townName]) || freshTownMood()
      const townMoodLabel = deriveDominantMoodLabel(townMood)
      const townFaction = findStoryFactionByTown(factions, town.townName)
      const townFactionName = townFaction ? townFaction.name : '-'
      const townFactionDoctrine = asText(
        townFaction?.doctrine,
        townFaction ? STORY_FACTION_DEFAULTS[townFaction.name]?.doctrine || '-' : '-',
        160
      )
      const townFactionRivals = townFaction
        ? normalizeStoryRivalNames(townFaction.rivals, townFaction.name)
        : []

      const rosterNames = new Map()
      for (const name of Object.keys(snapshot.agents || {})) {
        const safeName = asText(name, '', 80)
        if (!safeName) continue
        const key = safeName.toLowerCase()
        if (!rosterNames.has(key)) rosterNames.set(key, safeName)
      }
      for (const runtimeAgent of runtimeAgents) {
        const safeName = asText(runtimeAgent?.name, '', 80)
        if (!safeName) continue
        const key = safeName.toLowerCase()
        if (!rosterNames.has(key)) rosterNames.set(key, safeName)
      }
      const names = Array.from(rosterNames.values()).sort((a, b) => a.localeCompare(b))

      let population = 0
      const rosterLines = []
      for (const name of names) {
        const profile = snapshot.agents?.[name]?.profile
        const role = asText(profile?.job?.role, '', 20) || 'none'
        const homeMarker = asText(profile?.job?.home_marker, '', 80) || '-'
        const assignedAt = asText(profile?.job?.assigned_at, '', 80) || '-'
        const balance = Number(economy.ledger[name] || 0)
        const rep = normalizeAgentRep(profile?.rep)
        const factionRep = townFaction ? Number(rep[townFaction.name] || 0) : null
        if (sameText(homeMarker, town.marker.name, 80)) population += 1
        rosterLines.push(
          `GOD TOWN BOARD AGENT: name=${name} role=${role} home_marker=${homeMarker} assigned_at=${assignedAt} balance=${balance} rep_faction=${townFactionName} rep=${factionRep === null ? '-' : factionRep}`
        )
      }

      const marketsAtTown = markets
        .filter(market => sameText(market.marker, town.marker.name, 80))
        .sort((a, b) => a.name.localeCompare(b.name))
      const marketLines = marketsAtTown.map(market => {
        const offerCount = Array.isArray(market.offers) ? market.offers.length : 0
        return `GOD TOWN BOARD MARKET: name=${market.name} offers=${offerCount}`
      })

      const offerRows = marketsAtTown
        .flatMap(market => ((Array.isArray(market.offers) ? market.offers : [])
          .map(normalizeOffer)
          .filter(Boolean)
          .map(offer => ({
            marketName: market.name,
            offer
          }))))
        .sort((a, b) => {
          const diff = Number(b.offer.created_at || 0) - Number(a.offer.created_at || 0)
          if (diff !== 0) return diff
          const marketDiff = a.marketName.localeCompare(b.marketName)
          if (marketDiff !== 0) return marketDiff
          return a.offer.offer_id.localeCompare(b.offer.offer_id)
        })
        .slice(0, limit)
      const offerLines = offerRows.map(row => (
        `GOD TOWN BOARD OFFER: market=${row.marketName} id=${row.offer.offer_id} owner=${row.offer.owner} side=${row.offer.side} amount=${row.offer.amount} price=${row.offer.price} active=${row.offer.active} created_at=${row.offer.created_at}`
      ))

      const recentEvents = chronicle
        .filter(entry => chronicleMentionsTown(entry, town.townName))
        .sort((a, b) => {
          const diff = Number(b.at || 0) - Number(a.at || 0)
          if (diff !== 0) return diff
          return asText(a.id, '', 200).localeCompare(asText(b.id, '', 200))
        })
        .slice(0, limit)
      const eventLines = recentEvents.map(entry => (
        `GOD TOWN BOARD EVENT: at=${entry.at} type=${entry.type} msg=${entry.msg}`
      ))
      const quests = normalizeWorldQuests(snapshot.world?.quests)
      const sortQuests = (left, right) => {
        const diff = Date.parse(right.offered_at) - Date.parse(left.offered_at)
        if (diff !== 0) return diff
        return left.id.localeCompare(right.id)
      }
      const isQuestLinkedToTown = (quest) => {
        if (sameText(quest.town, town.townName, 80)) return true
        const ownerName = asText(quest.owner, '', 80)
        if (!ownerName) return false
        const ownerHomeMarker = asText(snapshot.agents?.[ownerName]?.profile?.job?.home_marker, '', 80)
        return sameText(ownerHomeMarker, town.marker.name, 80)
      }
      const mainAvailableQuestLines = quests
        .filter(quest => !isContractQuest(quest) && quest.type !== 'rumor_task' && quest.state === 'offered' && sameText(quest.town, town.townName, 80))
        .sort(sortQuests)
        .map(quest => (
          `GOD TOWN BOARD QUEST MAIN AVAILABLE: id=${quest.id} type=${quest.type} state=${quest.state} owner=${quest.owner || '-'} progress=${summarizeQuestProgress(quest)} reward=${quest.reward} title=${quest.title}`
        ))
      const mainActiveQuestLines = quests
        .filter((quest) => {
          if (isContractQuest(quest)) return false
          if (quest.type === 'rumor_task') return false
          if (!QUEST_ACTIVE_STATES.has(quest.state)) return false
          return isQuestLinkedToTown(quest)
        })
        .sort(sortQuests)
        .map(quest => (
          `GOD TOWN BOARD QUEST MAIN ACTIVE: id=${quest.id} type=${quest.type} state=${quest.state} owner=${quest.owner || '-'} progress=${summarizeQuestProgress(quest)} reward=${quest.reward} title=${quest.title}`
        ))
      const contractAvailableQuestLines = quests
        .filter((quest) => isContractQuest(quest) && quest.state === 'offered' && sameText(quest.town, town.townName, 80))
        .sort(sortQuests)
        .map(quest => (
          `GOD TOWN BOARD CONTRACT AVAILABLE: id=${quest.id} kind=${asText(quest.meta?.kind, 'contract', 40)} type=${quest.type} state=${quest.state} owner=${quest.owner || '-'} progress=${summarizeQuestProgress(quest)} reward=${quest.reward} risk=${asText(quest.meta?.risk, '-', 20)} title=${quest.title}`
        ))
      const contractActiveQuestLines = quests
        .filter((quest) => {
          if (!isContractQuest(quest)) return false
          if (!QUEST_ACTIVE_STATES.has(quest.state)) return false
          return isQuestLinkedToTown(quest)
        })
        .sort(sortQuests)
        .map(quest => (
          `GOD TOWN BOARD CONTRACT ACTIVE: id=${quest.id} kind=${asText(quest.meta?.kind, 'contract', 40)} type=${quest.type} state=${quest.state} owner=${quest.owner || '-'} progress=${summarizeQuestProgress(quest)} reward=${quest.reward} risk=${asText(quest.meta?.risk, '-', 20)} title=${quest.title}`
        ))
      const rumorLeadQuestAvailableLines = quests
        .filter(quest => quest.type === 'rumor_task' && quest.state === 'offered' && sameText(quest.town, town.townName, 80))
        .sort(sortQuests)
        .map(quest => (
          `GOD TOWN BOARD RUMOR LEAD QUEST AVAILABLE: id=${quest.id} type=${quest.type} state=${quest.state} owner=${quest.owner || '-'} rumor_id=${quest.rumor_id || '-'} progress=${summarizeQuestProgress(quest)} reward=${quest.reward} title=${quest.title}`
        ))
      const rumorLeadQuestActiveLines = quests
        .filter((quest) => {
          if (quest.type !== 'rumor_task') return false
          if (!QUEST_ACTIVE_STATES.has(quest.state)) return false
          return isQuestLinkedToTown(quest)
        })
        .sort(sortQuests)
        .map(quest => (
          `GOD TOWN BOARD RUMOR LEAD QUEST ACTIVE: id=${quest.id} type=${quest.type} state=${quest.state} owner=${quest.owner || '-'} rumor_id=${quest.rumor_id || '-'} progress=${summarizeQuestProgress(quest)} reward=${quest.reward} title=${quest.title}`
        ))
      const rumorLeadLines = normalizeWorldRumors(snapshot.world?.rumors)
        .filter((rumor) => sameText(rumor.town, town.townName, 80) && Number(rumor.expires_day || 0) >= clock.day)
        .sort((a, b) => {
          const diff = Number(b.created_at || 0) - Number(a.created_at || 0)
          if (diff !== 0) return diff
          return a.id.localeCompare(b.id)
        })
        .slice(0, limit)
        .map((rumor) => (
          `GOD TOWN BOARD RUMOR LEAD: id=${rumor.id} kind=${rumor.kind} severity=${rumor.severity} expires_day=${rumor.expires_day} text=${rumor.text}`
        ))
      const availableQuestLines = [...mainAvailableQuestLines]
      const activeQuestLines = [...mainActiveQuestLines]
      const pulse = getMarketPulse(town.townName, snapshot.world)
      const routeRisk = getRouteRisk(town.townName, snapshot.world)
      const traderTip = getTraderTip(town.townName, snapshot.world)
      const nether = normalizeNetherState(snapshot.world?.nether, deriveNetherSeed(snapshot.world))
      const latestNetherEvent = normalizeNetherLedger(nether.eventLedger).slice(-1)[0] || null
      const missionView = getTownMajorMissionView(snapshot.world, town.townName)
      const activeMajorMission = normalizeMajorMission(missionView?.activeMission)
      const availableMajorMission = normalizeMajorMission(missionView?.availableMission)
      const majorMissionLine = activeMajorMission
        ? `GOD TOWN BOARD MAJOR MISSION ACTIVE: id=${activeMajorMission.id} template=${activeMajorMission.templateId} phase=${activeMajorMission.phase} ${formatMajorMissionStakes(activeMajorMission)}`
        : availableMajorMission
          ? `GOD TOWN BOARD MAJOR MISSION TEASER: id=${availableMajorMission.id} template=${availableMajorMission.templateId} status=${availableMajorMission.status} ${formatMajorMissionStakes(availableMajorMission)}`
          : 'GOD TOWN BOARD MAJOR MISSION: (none)'
      const sideQuestLines = quests
        .filter((quest) => {
          if (!isQuestLinkedToTown(quest)) return false
          const origin = asText(quest.origin, '', 40).toLowerCase()
          const isSide = origin === 'townsfolk' || quest.type === 'rumor_task' || quest.meta?.side === true
          if (!isSide) return false
          return quest.state === 'offered' || QUEST_ACTIVE_STATES.has(quest.state)
        })
        .sort(sortQuests)
        .slice(0, Math.min(limit, 5))
        .map(quest => (
          `GOD TOWN BOARD SIDE QUEST: id=${quest.id} origin=${asText(quest.origin, quest.type === 'rumor_task' ? 'rumor' : 'quest', 40)} state=${quest.state} npc=${asText(quest.npcKey, '-', 80)} supports_major_mission_id=${asText(quest.supportsMajorMissionId, '-', 200)} progress=${summarizeQuestProgress(quest)} reward=${quest.reward} title=${quest.title}`
        ))

      const lines = [
        `GOD TOWN BOARD: town=${town.townName} marker=${town.marker.name} x=${town.marker.x} y=${town.marker.y} z=${town.marker.z} population=${population} limit=${limit}`,
        ...pulse.hot.map(item => `GOD TOWN BOARD MARKET PULSE HOT: good=${item.good} hint=${item.multiplierHint} reason=${item.reason}`),
        ...pulse.cold.map(item => `GOD TOWN BOARD MARKET PULSE COLD: good=${item.good} reason=${item.reason}`),
        `GOD TOWN BOARD ROUTE RISK: label=${routeRisk.label} reason=${routeRisk.reason} note=${routeRisk.nightPenaltyHint}`,
        `GOD TOWN BOARD TRADER TIP: ${traderTip}`,
        `GOD TOWN BOARD NETHER PULSE: ${formatNetherModifierSummary(nether)}`,
        latestNetherEvent
          ? `GOD TOWN BOARD NETHER EVENT: day=${latestNetherEvent.day} type=${latestNetherEvent.type} id=${latestNetherEvent.id}`
          : 'GOD TOWN BOARD NETHER EVENT: (none)',
        majorMissionLine,
        `GOD TOWN BOARD SIDE QUESTS TOP: count=${sideQuestLines.length}`,
        ...(sideQuestLines.length > 0 ? sideQuestLines : ['GOD TOWN BOARD SIDE QUEST: (none)']),
        `GOD TOWN BOARD CONTRACTS AVAILABLE: count=${contractAvailableQuestLines.length}`,
        ...(contractAvailableQuestLines.length > 0 ? contractAvailableQuestLines : ['GOD TOWN BOARD CONTRACT AVAILABLE: (none)']),
        `GOD TOWN BOARD CONTRACTS ACTIVE: count=${contractActiveQuestLines.length}`,
        ...(contractActiveQuestLines.length > 0 ? contractActiveQuestLines : ['GOD TOWN BOARD CONTRACT ACTIVE: (none)']),
        `GOD TOWN BOARD RUMOR LEAD QUESTS AVAILABLE: count=${rumorLeadQuestAvailableLines.length}`,
        ...(rumorLeadQuestAvailableLines.length > 0 ? rumorLeadQuestAvailableLines : ['GOD TOWN BOARD RUMOR LEAD QUEST AVAILABLE: (none)']),
        `GOD TOWN BOARD RUMOR LEAD QUESTS ACTIVE: count=${rumorLeadQuestActiveLines.length}`,
        ...(rumorLeadQuestActiveLines.length > 0 ? rumorLeadQuestActiveLines : ['GOD TOWN BOARD RUMOR LEAD QUEST ACTIVE: (none)']),
        `GOD TOWN BOARD RUMOR LEADS: count=${rumorLeadLines.length}`,
        ...(rumorLeadLines.length > 0 ? rumorLeadLines : ['GOD TOWN BOARD RUMOR LEAD: (none)']),
        `GOD TOWN BOARD CLOCK: day=${clock.day} phase=${clock.phase} season=${clock.season}`,
        `GOD TOWN BOARD THREAT: town=${town.townName} level=${townThreatLevel}`,
        `GOD TOWN BOARD MOOD: town=${town.townName} mood=${townMoodLabel} fear=${townMood.fear} unrest=${townMood.unrest} prosperity=${townMood.prosperity}`,
        `GOD TOWN BOARD FACTION: town=${town.townName} name=${townFactionName} doctrine=${townFactionDoctrine} rivals=${townFactionRivals.length ? townFactionRivals.join('|') : '-'}`,
        `GOD TOWN BOARD QUESTS AVAILABLE: count=${availableQuestLines.length}`,
        ...(availableQuestLines.length > 0 ? availableQuestLines.map(line => line.replace('QUEST MAIN AVAILABLE', 'QUEST AVAILABLE')) : ['GOD TOWN BOARD QUEST AVAILABLE: (none)']),
        `GOD TOWN BOARD QUESTS ACTIVE: count=${activeQuestLines.length}`,
        ...(activeQuestLines.length > 0 ? activeQuestLines.map(line => line.replace('QUEST MAIN ACTIVE', 'QUEST ACTIVE')) : ['GOD TOWN BOARD QUEST ACTIVE: (none)']),
        `GOD TOWN BOARD QUESTS MAIN AVAILABLE: count=${mainAvailableQuestLines.length}`,
        ...(mainAvailableQuestLines.length > 0 ? mainAvailableQuestLines : ['GOD TOWN BOARD QUEST MAIN AVAILABLE: (none)']),
        `GOD TOWN BOARD QUESTS MAIN ACTIVE: count=${mainActiveQuestLines.length}`,
        ...(mainActiveQuestLines.length > 0 ? mainActiveQuestLines : ['GOD TOWN BOARD QUEST MAIN ACTIVE: (none)']),
        `GOD TOWN BOARD ROSTER: count=${rosterLines.length}`,
        ...(rosterLines.length > 0 ? rosterLines : ['GOD TOWN BOARD AGENT: (none)']),
        `GOD TOWN BOARD MARKETS: count=${marketLines.length}`,
        ...(marketLines.length > 0 ? marketLines : ['GOD TOWN BOARD MARKET: (none)']),
        `GOD TOWN BOARD OFFERS: count=${offerLines.length}`,
        ...(offerLines.length > 0 ? offerLines : ['GOD TOWN BOARD OFFER: (none)']),
        `GOD TOWN BOARD EVENTS: count=${eventLines.length}`,
        ...(eventLines.length > 0 ? eventLines : ['GOD TOWN BOARD EVENT: (none)'])
      ]
      return { applied: true, command, audit: false, outputLines: lines }
    }

    if (parsed.type === 'mayor_talk') {
      const snapshot = memoryStore.getSnapshot()
      const townName = resolveTownName(snapshot.world, parsed.townName)
      if (!townName) return { applied: false, command, reason: 'Unknown town.' }

      const view = getTownMajorMissionView(snapshot.world, townName)
      if (!view) return { applied: false, command, reason: 'Unknown town.' }
      if (view.activeMission) {
        const activeMission = normalizeMajorMission(view.activeMission)
        const phaseNote = getMajorMissionPhaseNote(activeMission)
        const phaseNoteLine = phaseNote ? `GOD MAYOR TALK NOTE: ${phaseNote}` : null
        return {
          applied: true,
          command,
          audit: false,
          outputLines: [
            `GOD MAYOR TALK: town=${townName} status=active mission_id=${activeMission.id} phase=${activeMission.phase} ${formatMajorMissionStakes(activeMission)}`,
            ...(phaseNoteLine ? [phaseNoteLine] : [])
          ]
        }
      }
      if (view.availableMission) {
        const availableMission = normalizeMajorMission(view.availableMission)
        const teaser = buildMajorMissionTeaserMessage(townName, availableMission)
        const briefing = buildMajorMissionBriefingMessage(townName, availableMission)
        return {
          applied: true,
          command,
          audit: false,
          outputLines: [
            `GOD MAYOR TALK: town=${townName} status=${availableMission.status} mission_id=${availableMission.id} template=${availableMission.templateId}`,
            `GOD MAYOR TALK TEASER: ${teaser}`,
            `GOD MAYOR TALK BRIEFING: ${briefing}`
          ]
        }
      }
      if (Number(view.townState.majorMissionCooldownUntilDay || 0) > Number(view.day || 0)) {
        return {
          applied: false,
          command,
          reason: `Mayor cooldown active until day ${view.townState.majorMissionCooldownUntilDay}.`
        }
      }

      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const canonicalTown = resolveTownName(memory.world, parsed.townName)
          if (!canonicalTown) {
            throw new AppError({
              code: 'UNKNOWN_TOWN',
              message: `Unknown town for mayor talk: ${parsed.townName}`,
              recoverable: true
            })
          }
          enforceMajorMissionTownExclusivity(memory.world)
          const nextView = getTownMajorMissionView(memory.world, canonicalTown)
          if (!nextView) {
            throw new AppError({
              code: 'UNKNOWN_TOWN',
              message: `Unknown town for mayor talk: ${parsed.townName}`,
              recoverable: true
            })
          }
          if (nextView.activeMission || nextView.availableMission) {
            return normalizeMajorMission(nextView.activeMission || nextView.availableMission)
          }
          if (Number(nextView.townState.majorMissionCooldownUntilDay || 0) > Number(nextView.day || 0)) {
            throw new AppError({
              code: 'MISSION_COOLDOWN',
              message: `Cooldown active for ${canonicalTown}.`,
              recoverable: true
            })
          }

          const draftMission = createMajorMissionDraft(memory.world, canonicalTown)
          const missions = ensureWorldMajorMissions(memory.world)
          missions.push(draftMission)
          enforceMajorMissionTownExclusivity(memory.world)
          const at = now()
          appendMajorMissionAnnouncements(memory, {
            townName: canonicalTown,
            mission: draftMission,
            at,
            idPrefix: `${operationId}:mayor_talk:${canonicalTown.toLowerCase()}`,
            crierType: 'mission_available',
            message: buildMajorMissionBriefingMessage(canonicalTown, draftMission),
            status: 'briefed'
          })
          return draftMission
        }, { eventId: `${operationId}:mayor_talk:${townName.toLowerCase()}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'UNKNOWN_TOWN') return { applied: false, command, reason: 'Unknown town.' }
        if (err instanceof AppError && err.code === 'MISSION_COOLDOWN') {
          const latest = getTownMajorMissionView(memoryStore.getSnapshot().world, townName)
          return {
            applied: false,
            command,
            reason: `Mayor cooldown active until day ${latest?.townState?.majorMissionCooldownUntilDay || 0}.`
          }
        }
        throw err
      }
      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      const mission = normalizeMajorMission(tx.result)
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [
          `GOD MAYOR TALK: town=${townName} status=${mission.status} mission_id=${mission.id} template=${mission.templateId}`,
          `GOD MAYOR TALK TEASER: ${buildMajorMissionTeaserMessage(townName, mission)}`,
          `GOD MAYOR TALK BRIEFING: ${buildMajorMissionBriefingMessage(townName, mission)}`
        ]
      }
    }

    if (parsed.type === 'mayor_accept') {
      const snapshot = memoryStore.getSnapshot()
      const townName = resolveTownName(snapshot.world, parsed.townName)
      if (!townName) return { applied: false, command, reason: 'Unknown town.' }
      const view = getTownMajorMissionView(snapshot.world, townName)
      if (!view) return { applied: false, command, reason: 'Unknown town.' }
      if (view.activeMission) return { applied: false, command, reason: 'Major mission already active.' }
      if (Number(view.townState.majorMissionCooldownUntilDay || 0) > Number(view.day || 0)) {
        return {
          applied: false,
          command,
          reason: `Mayor cooldown active until day ${view.townState.majorMissionCooldownUntilDay}.`
        }
      }
      if (!view.availableMission) {
        return { applied: false, command, reason: 'No major mission briefing is available. Talk to the mayor first.' }
      }

      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const canonicalTown = resolveTownName(memory.world, parsed.townName)
          if (!canonicalTown) {
            throw new AppError({
              code: 'UNKNOWN_TOWN',
              message: `Unknown town for mayor accept: ${parsed.townName}`,
              recoverable: true
            })
          }
          enforceMajorMissionTownExclusivity(memory.world)
          const nextView = getTownMajorMissionView(memory.world, canonicalTown)
          if (!nextView) {
            throw new AppError({
              code: 'UNKNOWN_TOWN',
              message: `Unknown town for mayor accept: ${parsed.townName}`,
              recoverable: true
            })
          }
          if (nextView.activeMission) {
            throw new AppError({
              code: 'MISSION_ALREADY_ACTIVE',
              message: `Major mission already active for ${canonicalTown}.`,
              recoverable: true
            })
          }
          if (Number(nextView.townState.majorMissionCooldownUntilDay || 0) > Number(nextView.day || 0)) {
            throw new AppError({
              code: 'MISSION_COOLDOWN',
              message: `Cooldown active for ${canonicalTown}.`,
              recoverable: true
            })
          }
          const availableMission = normalizeMajorMission(nextView.availableMission)
          if (!availableMission) {
            throw new AppError({
              code: 'MISSION_UNAVAILABLE',
              message: `No briefed mission for ${canonicalTown}.`,
              recoverable: true
            })
          }
          const missions = ensureWorldMajorMissions(memory.world)
          const idx = missions.findIndex(item => sameText(item?.id, availableMission.id, 200))
          if (idx < 0) {
            throw new AppError({
              code: 'MISSION_UNAVAILABLE',
              message: `No briefed mission for ${canonicalTown}.`,
              recoverable: true
            })
          }
          const mission = normalizeMajorMission(missions[idx])
          if (!mission || (mission.status !== 'briefed' && mission.status !== 'teased')) {
            throw new AppError({
              code: 'MISSION_UNAVAILABLE',
              message: `No briefed mission for ${canonicalTown}.`,
              recoverable: true
            })
          }
          const clock = ensureWorldClock(memory.world)
          mission.status = 'active'
          mission.phase = MAJOR_MISSION_PHASE_START
          mission.acceptedAtDay = clock.day
          mission.progress = normalizeMajorMissionPayload({
            ...mission.progress,
            advances: Number(mission.progress?.advances || 0),
            acceptedAtDay: clock.day
          })
          missions[idx] = mission
          const towns = ensureWorldTownMissionStates(memory.world)
          if (!Object.prototype.hasOwnProperty.call(towns, canonicalTown)) {
            towns[canonicalTown] = normalizeTownMissionState(null)
          }
          towns[canonicalTown].activeMajorMissionId = mission.id
          enforceMajorMissionTownExclusivity(memory.world)
          const at = now()
          appendMajorMissionAnnouncements(memory, {
            townName: canonicalTown,
            mission,
            at,
            idPrefix: `${operationId}:mayor_accept:${canonicalTown.toLowerCase()}`,
            crierType: 'mission_accepted',
            message: buildMajorMissionAcceptanceMessage(canonicalTown, mission),
            status: 'active'
          })
          return mission
        }, { eventId: `${operationId}:mayor_accept:${townName.toLowerCase()}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'UNKNOWN_TOWN') return { applied: false, command, reason: 'Unknown town.' }
        if (err instanceof AppError && err.code === 'MISSION_ALREADY_ACTIVE') return { applied: false, command, reason: 'Major mission already active.' }
        if (err instanceof AppError && err.code === 'MISSION_COOLDOWN') {
          const latest = getTownMajorMissionView(memoryStore.getSnapshot().world, townName)
          return {
            applied: false,
            command,
            reason: `Mayor cooldown active until day ${latest?.townState?.majorMissionCooldownUntilDay || 0}.`
          }
        }
        if (err instanceof AppError && err.code === 'MISSION_UNAVAILABLE') {
          return { applied: false, command, reason: 'No major mission briefing is available. Talk to the mayor first.' }
        }
        throw err
      }
      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [
          `GOD MAYOR ACCEPT: town=${townName} mission_id=${tx.result.id} phase=${tx.result.phase} status=${tx.result.status}`,
          `GOD MAYOR ACCEPT BRIEF: ${buildMajorMissionAcceptanceMessage(townName, tx.result)}`
        ]
      }
    }

    if (parsed.type === 'mission_status') {
      const snapshot = memoryStore.getSnapshot()
      const townName = resolveTownName(snapshot.world, parsed.townName)
      if (!townName) return { applied: false, command, reason: 'Unknown town.' }
      const view = getTownMajorMissionView(snapshot.world, townName)
      if (!view) return { applied: false, command, reason: 'Unknown town.' }
      const crierQueue = normalizeTownCrierQueue(view.townState?.crierQueue)
      const lines = [
        `GOD MISSION STATUS: town=${townName} day=${view.day} cooldown_until_day=${view.townState.majorMissionCooldownUntilDay} active_mission=${view.townState.activeMajorMissionId || '-'}`
      ]
      if (view.activeMission) {
        lines.push(
          `GOD MISSION ACTIVE: id=${view.activeMission.id} template=${view.activeMission.templateId} phase=${view.activeMission.phase} ${formatMajorMissionStakes(view.activeMission)}`
        )
      } else if (view.availableMission) {
        lines.push(
          `GOD MISSION AVAILABLE: id=${view.availableMission.id} status=${view.availableMission.status} template=${view.availableMission.templateId} ${formatMajorMissionStakes(view.availableMission)}`
        )
      } else {
        lines.push('GOD MISSION: (none)')
      }
      const recentCrier = crierQueue.slice(-3)
      lines.push(`GOD MISSION CRIER: count=${crierQueue.length}`)
      if (recentCrier.length > 0) {
        for (const item of recentCrier) {
          lines.push(`GOD MISSION CRIER ITEM: day=${item.day} type=${item.type} mission_id=${item.missionId || '-'} message=${item.message}`)
        }
      }
      return { applied: true, command, audit: false, outputLines: lines }
    }

    if (parsed.type === 'mission_advance') {
      const snapshot = memoryStore.getSnapshot()
      const townName = resolveTownName(snapshot.world, parsed.townName)
      if (!townName) return { applied: false, command, reason: 'Unknown town.' }
      const view = getTownMajorMissionView(snapshot.world, townName)
      if (!view || !view.activeMission) return { applied: false, command, reason: 'No active major mission.' }
      const phase = Number(view.activeMission.phase)
      if (!Number.isInteger(phase) || phase < MAJOR_MISSION_PHASE_START) {
        return { applied: false, command, reason: 'Active mission phase is invalid.' }
      }
      if (phase >= MAJOR_MISSION_PHASE_MAX) {
        return { applied: false, command, reason: 'Major mission is already at final phase.' }
      }

      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const canonicalTown = resolveTownName(memory.world, parsed.townName)
          if (!canonicalTown) {
            throw new AppError({
              code: 'UNKNOWN_TOWN',
              message: `Unknown town for mission advance: ${parsed.townName}`,
              recoverable: true
            })
          }
          enforceMajorMissionTownExclusivity(memory.world)
          const nextView = getTownMajorMissionView(memory.world, canonicalTown)
          const activeMission = normalizeMajorMission(nextView?.activeMission)
          if (!activeMission) {
            throw new AppError({
              code: 'MISSION_NOT_ACTIVE',
              message: `No active mission for ${canonicalTown}.`,
              recoverable: true
            })
          }
          const currentPhase = Number(activeMission.phase)
          if (!Number.isInteger(currentPhase) || currentPhase < MAJOR_MISSION_PHASE_START) {
            throw new AppError({
              code: 'MISSION_PHASE_INVALID',
              message: `Mission phase invalid for ${activeMission.id}.`,
              recoverable: true
            })
          }
          if (currentPhase >= MAJOR_MISSION_PHASE_MAX) {
            throw new AppError({
              code: 'MISSION_PHASE_FINAL',
              message: `Mission ${activeMission.id} already at final phase.`,
              recoverable: true
            })
          }
          const missions = ensureWorldMajorMissions(memory.world)
          const idx = missions.findIndex(item => sameText(item?.id, activeMission.id, 200))
          if (idx < 0) {
            throw new AppError({
              code: 'MISSION_NOT_ACTIVE',
              message: `No active mission for ${canonicalTown}.`,
              recoverable: true
            })
          }
          const mission = normalizeMajorMission(missions[idx])
          mission.phase = currentPhase + 1
          const clock = ensureWorldClock(memory.world)
          mission.progress = normalizeMajorMissionPayload({
            ...mission.progress,
            advances: Number(mission.progress?.advances || 0) + 1,
            lastAdvancedDay: clock.day
          })
          missions[idx] = mission
          enforceMajorMissionTownExclusivity(memory.world)
          const at = now()
          appendMajorMissionAnnouncements(memory, {
            townName: canonicalTown,
            mission,
            at,
            idPrefix: `${operationId}:mission_advance:${canonicalTown.toLowerCase()}`,
            crierType: 'mission_phase',
            message: buildMajorMissionPhaseChangeMessage(canonicalTown, mission),
            status: mission.status
          })
          return mission
        }, { eventId: `${operationId}:mission_advance:${townName.toLowerCase()}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'UNKNOWN_TOWN') return { applied: false, command, reason: 'Unknown town.' }
        if (err instanceof AppError && err.code === 'MISSION_NOT_ACTIVE') return { applied: false, command, reason: 'No active major mission.' }
        if (err instanceof AppError && err.code === 'MISSION_PHASE_INVALID') return { applied: false, command, reason: 'Active mission phase is invalid.' }
        if (err instanceof AppError && err.code === 'MISSION_PHASE_FINAL') return { applied: false, command, reason: 'Major mission is already at final phase.' }
        throw err
      }
      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [
          `GOD MISSION ADVANCE: town=${townName} mission_id=${tx.result.id} phase=${tx.result.phase} status=${tx.result.status}`,
          `GOD MISSION ADVANCE NOTE: ${buildMajorMissionPhaseChangeMessage(townName, tx.result)}`
        ]
      }
    }

    if (parsed.type === 'mission_complete') {
      const snapshot = memoryStore.getSnapshot()
      const townName = resolveTownName(snapshot.world, parsed.townName)
      if (!townName) return { applied: false, command, reason: 'Unknown town.' }
      const view = getTownMajorMissionView(snapshot.world, townName)
      if (!view || !view.activeMission) return { applied: false, command, reason: 'No active major mission.' }

      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const canonicalTown = resolveTownName(memory.world, parsed.townName)
          if (!canonicalTown) {
            throw new AppError({
              code: 'UNKNOWN_TOWN',
              message: `Unknown town for mission complete: ${parsed.townName}`,
              recoverable: true
            })
          }
          enforceMajorMissionTownExclusivity(memory.world)
          const nextView = getTownMajorMissionView(memory.world, canonicalTown)
          const activeMission = normalizeMajorMission(nextView?.activeMission)
          if (!activeMission) {
            throw new AppError({
              code: 'MISSION_NOT_ACTIVE',
              message: `No active mission for ${canonicalTown}.`,
              recoverable: true
            })
          }
          const missions = ensureWorldMajorMissions(memory.world)
          const idx = missions.findIndex(item => sameText(item?.id, activeMission.id, 200))
          if (idx < 0) {
            throw new AppError({
              code: 'MISSION_NOT_ACTIVE',
              message: `No active mission for ${canonicalTown}.`,
              recoverable: true
            })
          }
          const mission = normalizeMajorMission(missions[idx])
          const clock = ensureWorldClock(memory.world)
          mission.status = 'completed'
          mission.progress = normalizeMajorMissionPayload({
            ...mission.progress,
            completedAtDay: clock.day
          })
          missions[idx] = mission
          const towns = ensureWorldTownMissionStates(memory.world)
          if (!Object.prototype.hasOwnProperty.call(towns, canonicalTown)) {
            towns[canonicalTown] = normalizeTownMissionState(null)
          }
          towns[canonicalTown].activeMajorMissionId = null
          towns[canonicalTown].majorMissionCooldownUntilDay = clock.day + MAJOR_MISSION_COOLDOWN_DAYS
          enforceMajorMissionTownExclusivity(memory.world)
          const at = now()
          appendMajorMissionAnnouncements(memory, {
            townName: canonicalTown,
            mission,
            at,
            idPrefix: `${operationId}:mission_complete:${canonicalTown.toLowerCase()}`,
            crierType: 'mission_win',
            message: buildMajorMissionCompletionMessage(canonicalTown, mission),
            status: 'completed'
          })
          return {
            mission,
            cooldownUntilDay: towns[canonicalTown].majorMissionCooldownUntilDay
          }
        }, { eventId: `${operationId}:mission_complete:${townName.toLowerCase()}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'UNKNOWN_TOWN') return { applied: false, command, reason: 'Unknown town.' }
        if (err instanceof AppError && err.code === 'MISSION_NOT_ACTIVE') return { applied: false, command, reason: 'No active major mission.' }
        throw err
      }
      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [
          `GOD MISSION COMPLETE: town=${townName} mission_id=${tx.result.mission.id} status=${tx.result.mission.status} cooldown_until_day=${tx.result.cooldownUntilDay}`,
          `GOD MISSION COMPLETE NOTE: ${buildMajorMissionCompletionMessage(townName, tx.result.mission)}`
        ]
      }
    }

    if (parsed.type === 'mission_fail') {
      const snapshot = memoryStore.getSnapshot()
      const townName = resolveTownName(snapshot.world, parsed.townName)
      if (!townName) return { applied: false, command, reason: 'Unknown town.' }
      const view = getTownMajorMissionView(snapshot.world, townName)
      if (!view || !view.activeMission) return { applied: false, command, reason: 'No active major mission.' }
      const failReason = asText(parsed.reason, '', 160) || null

      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const canonicalTown = resolveTownName(memory.world, parsed.townName)
          if (!canonicalTown) {
            throw new AppError({
              code: 'UNKNOWN_TOWN',
              message: `Unknown town for mission fail: ${parsed.townName}`,
              recoverable: true
            })
          }
          enforceMajorMissionTownExclusivity(memory.world)
          const nextView = getTownMajorMissionView(memory.world, canonicalTown)
          const activeMission = normalizeMajorMission(nextView?.activeMission)
          if (!activeMission) {
            throw new AppError({
              code: 'MISSION_NOT_ACTIVE',
              message: `No active mission for ${canonicalTown}.`,
              recoverable: true
            })
          }
          const missions = ensureWorldMajorMissions(memory.world)
          const idx = missions.findIndex(item => sameText(item?.id, activeMission.id, 200))
          if (idx < 0) {
            throw new AppError({
              code: 'MISSION_NOT_ACTIVE',
              message: `No active mission for ${canonicalTown}.`,
              recoverable: true
            })
          }
          const mission = normalizeMajorMission(missions[idx])
          const clock = ensureWorldClock(memory.world)
          mission.status = 'failed'
          mission.progress = normalizeMajorMissionPayload({
            ...mission.progress,
            failedAtDay: clock.day,
            ...(failReason ? { failReason } : {})
          })
          missions[idx] = mission
          const towns = ensureWorldTownMissionStates(memory.world)
          if (!Object.prototype.hasOwnProperty.call(towns, canonicalTown)) {
            towns[canonicalTown] = normalizeTownMissionState(null)
          }
          towns[canonicalTown].activeMajorMissionId = null
          towns[canonicalTown].majorMissionCooldownUntilDay = clock.day + MAJOR_MISSION_COOLDOWN_DAYS
          enforceMajorMissionTownExclusivity(memory.world)
          const at = now()
          appendMajorMissionAnnouncements(memory, {
            townName: canonicalTown,
            mission,
            at,
            idPrefix: `${operationId}:mission_fail:${canonicalTown.toLowerCase()}`,
            crierType: 'mission_fail',
            message: buildMajorMissionFailureMessage(canonicalTown, mission, failReason),
            status: 'failed'
          })
          return {
            mission,
            cooldownUntilDay: towns[canonicalTown].majorMissionCooldownUntilDay
          }
        }, { eventId: `${operationId}:mission_fail:${townName.toLowerCase()}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'UNKNOWN_TOWN') return { applied: false, command, reason: 'Unknown town.' }
        if (err instanceof AppError && err.code === 'MISSION_NOT_ACTIVE') return { applied: false, command, reason: 'No active major mission.' }
        throw err
      }
      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [
          `GOD MISSION FAIL: town=${townName} mission_id=${tx.result.mission.id} status=${tx.result.mission.status} cooldown_until_day=${tx.result.cooldownUntilDay} reason=${failReason || '-'}`,
          `GOD MISSION FAIL NOTE: ${buildMajorMissionFailureMessage(townName, tx.result.mission, failReason)}`
        ]
      }
    }

    if (parsed.type === 'nether_status') {
      const snapshot = memoryStore.getSnapshot()
      const nether = normalizeNetherState(snapshot.world?.nether, deriveNetherSeed(snapshot.world))
      const recent = normalizeNetherLedger(nether.eventLedger).slice(-5)
      const lines = [
        `GOD NETHER STATUS: seed=${nether.deckState.seed} cursor=${nether.deckState.cursor} last_tick_day=${nether.lastTickDay} ledger_count=${nether.eventLedger.length}`,
        `GOD NETHER MODIFIERS: ${formatNetherModifierSummary(nether)}`,
        `GOD NETHER LEDGER: count=${recent.length}`
      ]
      if (recent.length === 0) {
        lines.push('GOD NETHER LEDGER ITEM: (none)')
      } else {
        for (const entry of recent) {
          lines.push(`GOD NETHER LEDGER ITEM: id=${entry.id} day=${entry.day} type=${entry.type} applied=${entry.applied}`)
        }
      }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: lines
      }
    }

    if (parsed.type === 'nether_tick') {
      const nDays = Math.trunc(Number(parsed.nDays || 1))
      if (!Number.isInteger(nDays) || nDays < 1 || nDays > 1000) {
        return { applied: false, command, reason: 'Invalid nDays.' }
      }
      const tx = await memoryStore.transact((memory) => {
        const at = now()
        const result = advanceNetherByDays(memory, {
          nDays,
          at,
          idPrefix: `${operationId}:nether_tick`
        })
        const nether = ensureWorldNether(memory.world)
        return {
          nDays,
          applied: result.applied,
          lastTickDay: nether.lastTickDay,
          modifiers: normalizeNetherModifiers(nether.modifiers)
        }
      }, { eventId: `${operationId}:nether_tick:${nDays}` })
      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [
          `GOD NETHER TICK: n_days=${tx.result.nDays} applied_events=${tx.result.applied.length} last_tick_day=${tx.result.lastTickDay}`,
          `GOD NETHER MODIFIERS: longNight=${tx.result.modifiers.longNight} omen=${tx.result.modifiers.omen} scarcity=${tx.result.modifiers.scarcity} threat=${tx.result.modifiers.threat}`
        ]
      }
    }

    if (parsed.type === 'townsfolk_talk') {
      const snapshot = memoryStore.getSnapshot()
      const townName = resolveTownName(snapshot.world, parsed.townName)
      if (!townName) return { applied: false, command, reason: 'Unknown town.' }
      const npcKey = normalizeNpcKey(parsed.npcName)
      if (!npcKey) return { applied: false, command, reason: 'Invalid npc.' }

      const tx = await memoryStore.transact((memory) => {
        const canonicalTown = resolveTownName(memory.world, parsed.townName)
        if (!canonicalTown) {
          throw new AppError({
            code: 'UNKNOWN_TOWN',
            message: `Unknown town for townsfolk talk: ${parsed.townName}`,
            recoverable: true
          })
        }
        const questDraft = normalizeQuest(buildTownsfolkQuestDraft(memory.world, canonicalTown, parsed.npcName))
        if (!questDraft) {
          throw new AppError({
            code: 'QUEST_BUILD_FAILED',
            message: `Unable to build townsfolk quest for ${canonicalTown}.`,
            recoverable: true
          })
        }
        const quests = ensureWorldQuests(memory.world)
        const already = findQuestById(quests, questDraft.id)
        if (already) return { quest: already, created: false }
        quests.push(questDraft)
        memory.world.quests = boundTownsfolkQuestHistory(quests)
        const at = now()
        const msg = `[${canonicalTown}] TOWNSFOLK: ${parsed.npcName} posted side quest ${questDraft.id}.`
        appendChronicle(memory, {
          id: `${operationId}:chronicle:townsfolk_talk:${questDraft.id.toLowerCase()}`,
          type: 'quest_offer',
          msg,
          at,
          town: canonicalTown,
          meta: {
            quest_id: questDraft.id,
            origin: 'townsfolk',
            npc: parsed.npcName
          }
        })
        appendNews(memory, {
          id: `${operationId}:news:townsfolk_talk:${questDraft.id.toLowerCase()}`,
          topic: 'quest',
          msg,
          at,
          town: canonicalTown,
          meta: {
            quest_id: questDraft.id,
            origin: 'townsfolk',
            npc: parsed.npcName
          }
        })
        return { quest: questDraft, created: true }
      }, { eventId: `${operationId}:townsfolk_talk:${townName.toLowerCase()}:${npcKey}` })
      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      if (!tx.result?.quest) return { applied: false, command, reason: 'No townsfolk quest available.' }
      return {
        applied: true,
        command,
        audit: tx.result.created === true,
        outputLines: [
          `GOD TOWNSFOLK TALK: town=${townName} npc=${npcKey} status=${tx.result.created ? 'created' : 'existing'} quest_id=${tx.result.quest.id} type=${tx.result.quest.type} state=${tx.result.quest.state}`,
          `GOD TOWNSFOLK QUEST: supports_major_mission_id=${asText(tx.result.quest.supportsMajorMissionId, '-', 200)} reward=${tx.result.quest.reward} title=${tx.result.quest.title}`
        ]
      }
    }

    if (parsed.type === 'chronicle_tail') {
      const snapshot = memoryStore.getSnapshot()
      const chronicle = normalizeWorldChronicle(snapshot.world?.chronicle)
      const limit = Math.max(1, Math.min(200, Number(parsed.limit || 10)))
      const rows = chronicle.slice(-limit)
      if (rows.length === 0) {
        return { applied: true, command, audit: false, outputLines: ['GOD CHRONICLE TAIL: (none)'] }
      }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD CHRONICLE TAIL: count=${rows.length} total=${chronicle.length}`,
          ...rows.map(entry => (
            `GOD CHRONICLE: id=${entry.id} type=${entry.type} town=${entry.town || '-'} at=${entry.at} msg=${entry.msg}`
          ))
        ]
      }
    }

    if (parsed.type === 'chronicle_grep') {
      const snapshot = memoryStore.getSnapshot()
      const chronicle = normalizeWorldChronicle(snapshot.world?.chronicle)
      const term = parsed.term.toLowerCase()
      const limit = Math.max(1, Math.min(200, Number(parsed.limit || 10)))
      const rows = chronicle.filter(entry => (
        asText(entry.type, '', 40).toLowerCase().includes(term)
        || asText(entry.msg, '', 240).toLowerCase().includes(term)
        || asText(entry.town, '', 80).toLowerCase().includes(term)
      )).slice(-limit)
      if (rows.length === 0) {
        return { applied: true, command, audit: false, outputLines: [`GOD CHRONICLE GREP: term=${parsed.term} (none)`] }
      }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD CHRONICLE GREP: term=${parsed.term} count=${rows.length}`,
          ...rows.map(entry => (
            `GOD CHRONICLE: id=${entry.id} type=${entry.type} town=${entry.town || '-'} at=${entry.at} msg=${entry.msg}`
          ))
        ]
      }
    }

    if (parsed.type === 'news_tail') {
      const snapshot = memoryStore.getSnapshot()
      const news = normalizeWorldNews(snapshot.world?.news)
      const limit = Math.max(1, Math.min(200, Number(parsed.limit || 10)))
      const rows = news.slice(-limit)
      if (rows.length === 0) {
        return { applied: true, command, audit: false, outputLines: ['GOD NEWS TAIL: (none)'] }
      }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD NEWS TAIL: count=${rows.length} total=${news.length}`,
          ...rows.map(entry => (
            `GOD NEWS: id=${entry.id} topic=${entry.topic} town=${entry.town || '-'} at=${entry.at} msg=${entry.msg}`
          ))
        ]
      }
    }

    if (parsed.type === 'chronicle_add') {
      const snapshot = memoryStore.getSnapshot()
      const towns = deriveTownsFromMarkers(snapshot.world?.markers || [])
      const rawTokens = [...parsed.tokens]
      const explicitTownMatch = /^town=(.+)$/i.exec(rawTokens[rawTokens.length - 1] || '')
      let townName = ''
      if (explicitTownMatch) {
        rawTokens.pop()
        const explicitTown = asText(explicitTownMatch[1], '', 80)
        const resolvedTown = findTownByName(towns, explicitTown)
        if (!resolvedTown) return { applied: false, command, reason: 'Unknown town.' }
        townName = resolvedTown.townName
      } else if (rawTokens.length >= 2) {
        const maybeTown = findTownByName(towns, rawTokens[rawTokens.length - 1] || '')
        if (maybeTown) {
          rawTokens.pop()
          townName = maybeTown.townName
        }
      }

      const message = asText(rawTokens.join(' '), '', 240)
      if (!message) return { applied: false, command, reason: 'Message is required.' }

      const tx = await memoryStore.transact((memory) => {
        const at = now()
        appendChronicle(memory, {
          id: `${operationId}:chronicle_add:${parsed.eventType}`,
          type: parsed.eventType,
          msg: message,
          at,
          town: townName || undefined
        })
        return {
          eventType: parsed.eventType,
          message,
          town: townName || null,
          at
        }
      }, { eventId: `${operationId}:chronicle_add:${parsed.eventType}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [
          `GOD CHRONICLE ADD: type=${tx.result.eventType} town=${tx.result.town || '-'} at=${tx.result.at} msg=${tx.result.message}`
        ]
      }
    }

    if (parsed.type === 'contract_list') {
      const snapshot = memoryStore.getSnapshot()
      const towns = deriveTownsFromMarkers(snapshot.world?.markers || [])
      const townFilter = parsed.townName ? findTownByName(towns, parsed.townName) : null
      if (parsed.townName && !townFilter) return { applied: false, command, reason: 'Unknown town.' }
      const contracts = normalizeWorldQuests(snapshot.world?.quests)
        .filter((quest) => {
          if (!isContractQuest(quest)) return false
          return !townFilter || sameText(quest.town, townFilter.townName, 80)
        })
        .sort((a, b) => {
          const diff = Date.parse(b.offered_at) - Date.parse(a.offered_at)
          if (diff !== 0) return diff
          return a.id.localeCompare(b.id)
        })
      if (contracts.length === 0) {
        return {
          applied: true,
          command,
          audit: false,
          outputLines: [`GOD CONTRACT LIST: town=${townFilter ? townFilter.townName : '-'} (none)`]
        }
      }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD CONTRACT LIST: town=${townFilter ? townFilter.townName : '-'} count=${contracts.length}`,
          ...contracts.map((quest) => (
            `GOD CONTRACT: id=${quest.id} kind=${asText(quest.meta?.kind, 'contract', 40)} type=${quest.type} state=${quest.state} town=${quest.town || '-'} owner=${quest.owner || '-'} progress=${summarizeQuestProgress(quest)} reward=${quest.reward} risk=${asText(quest.meta?.risk, '-', 20)} title=${quest.title}`
          ))
        ]
      }
    }

    if (parsed.type === 'contract_show') {
      const snapshot = memoryStore.getSnapshot()
      const quest = findContractQuestById(snapshot.world?.quests || [], parsed.questId)
      if (!quest) return { applied: false, command, reason: 'Unknown contract.' }
      let objectiveLine = '-'
      let progressLine = '-'
      if (quest.type === 'trade_n') {
        objectiveLine = `kind=trade_n n=${quest.objective.n} market=${quest.objective.market || '-'}`
        progressLine = `done=${quest.progress.done}`
      } else if (quest.type === 'visit_town') {
        objectiveLine = `kind=visit_town town=${quest.objective.town}`
        progressLine = `visited=${quest.progress.visited}`
      }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD CONTRACT SHOW: id=${quest.id} kind=${asText(quest.meta?.kind, 'contract', 40)} state=${quest.state} town=${quest.town || '-'} owner=${quest.owner || '-'} reward=${quest.reward}`,
          `GOD CONTRACT TITLE: ${quest.title}`,
          `GOD CONTRACT DESC: ${quest.desc}`,
          `GOD CONTRACT OBJECTIVE: ${objectiveLine}`,
          `GOD CONTRACT PROGRESS: ${progressLine}`,
          `GOD CONTRACT RISK: label=${asText(quest.meta?.risk, '-', 20)} note=${asText(quest.meta?.risk_note, '-', 160)}`,
          `GOD CONTRACT TIMES: offered_at=${quest.offered_at} accepted_at=${quest.accepted_at || '-'}`
        ]
      }
    }

    if (parsed.type === 'contract_accept') {
      const snapshot = memoryStore.getSnapshot()
      const contract = findContractQuestById(snapshot.world?.quests || [], parsed.questId)
      if (!contract) return { applied: false, command, reason: 'Unknown contract.' }
      const ownerName = parsed.autoAssign
        ? resolveDefaultContractOwner(snapshot, runtimeAgents)
        : resolveKnownAgentName(snapshot, runtimeAgents, parsed.agentName)
      if (!ownerName) return { applied: false, command, reason: 'Unknown agent.' }
      const delegated = await applyGodCommand({
        agents: runtimeAgents,
        command: `quest accept ${ownerName} ${contract.id}`,
        operationId: `${operationId}:contract_accept`
      })
      if (!delegated.applied) return { applied: false, command, reason: delegated.reason }
      const appliedQuest = findQuestById(memoryStore.getSnapshot().world?.quests || [], contract.id)
      return {
        applied: true,
        command,
        audit: delegated.audit,
        outputLines: [
          `GOD CONTRACT ACCEPTED: id=${contract.id} owner=${appliedQuest?.owner || ownerName} state=${appliedQuest?.state || '-'}`
        ]
      }
    }

    if (parsed.type === 'contract_complete') {
      const snapshot = memoryStore.getSnapshot()
      const contract = findContractQuestById(snapshot.world?.quests || [], parsed.questId)
      if (!contract) return { applied: false, command, reason: 'Unknown contract.' }
      const delegated = await applyGodCommand({
        agents: runtimeAgents,
        command: `quest complete ${contract.id}`,
        operationId: `${operationId}:contract_complete`
      })
      if (!delegated.applied) return { applied: false, command, reason: delegated.reason }
      const appliedQuest = findQuestById(memoryStore.getSnapshot().world?.quests || [], contract.id)
      return {
        applied: true,
        command,
        audit: delegated.audit,
        outputLines: [
          `GOD CONTRACT COMPLETED: id=${contract.id} owner=${appliedQuest?.owner || '-'} reward=${appliedQuest?.reward || 0}`
        ]
      }
    }

    if (parsed.type === 'quest_list') {
      const snapshot = memoryStore.getSnapshot()
      const towns = deriveTownsFromMarkers(snapshot.world?.markers || [])
      const townFilter = parsed.townName ? findTownByName(towns, parsed.townName) : null
      if (parsed.townName && !townFilter) return { applied: false, command, reason: 'Unknown town.' }
      const quests = normalizeWorldQuests(snapshot.world?.quests)
        .filter(quest => !townFilter || sameText(quest.town, townFilter.townName, 80))
        .sort((a, b) => {
          const diff = Date.parse(b.offered_at) - Date.parse(a.offered_at)
          if (diff !== 0) return diff
          return a.id.localeCompare(b.id)
        })
      if (quests.length === 0) {
        return {
          applied: true,
          command,
          audit: false,
          outputLines: [`GOD QUEST LIST: town=${townFilter ? townFilter.townName : '-'} (none)`]
        }
      }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD QUEST LIST: town=${townFilter ? townFilter.townName : '-'} count=${quests.length}`,
          ...quests.map(quest => (
            `GOD QUEST: id=${quest.id} type=${quest.type} origin=${asText(quest.origin, '-', 40)} state=${quest.state} town=${quest.town || '-'} owner=${quest.owner || '-'} progress=${summarizeQuestProgress(quest)} reward=${quest.reward} title=${quest.title}`
          ))
        ]
      }
    }

    if (parsed.type === 'quest_show') {
      const snapshot = memoryStore.getSnapshot()
      const quest = findQuestById(snapshot.world?.quests || [], parsed.questId)
      if (!quest) return { applied: false, command, reason: 'Unknown quest.' }
      let objectiveLine = '-'
      let progressLine = '-'
      if (quest.type === 'trade_n') {
        objectiveLine = `kind=trade_n n=${quest.objective.n} market=${quest.objective.market || '-'}`
        progressLine = `done=${quest.progress.done}`
      } else if (quest.type === 'visit_town') {
        objectiveLine = `kind=visit_town town=${quest.objective.town}`
        progressLine = `visited=${quest.progress.visited}`
      } else if (quest.type === 'rumor_task') {
        objectiveLine = `kind=rumor_task rumor_task=${quest.objective.rumor_task} rumor_id=${quest.objective.rumor_id} n=${quest.objective.n || '-'} town=${quest.objective.town || '-'} market=${quest.objective.market || '-'}`
        progressLine = typeof quest.progress.done === 'number'
          ? `done=${quest.progress.done}`
          : `visited=${quest.progress.visited}`
      }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD QUEST SHOW: id=${quest.id} type=${quest.type} origin=${asText(quest.origin, '-', 40)} state=${quest.state} town=${quest.town || '-'} town_id=${asText(quest.townId, '-', 80)} npc=${asText(quest.npcKey, '-', 80)} supports_major_mission_id=${asText(quest.supportsMajorMissionId, '-', 200)} owner=${quest.owner || '-'} reward=${quest.reward} rumor_id=${quest.rumor_id || '-'}`,
          `GOD QUEST TITLE: ${quest.title}`,
          `GOD QUEST DESC: ${quest.desc}`,
          `GOD QUEST OBJECTIVE: ${objectiveLine}`,
          `GOD QUEST PROGRESS: ${progressLine}`,
          `GOD QUEST TIMES: offered_at=${quest.offered_at} accepted_at=${quest.accepted_at || '-'}`
        ]
      }
    }

    if (parsed.type === 'quest_offer_trade_n') {
      const snapshot = memoryStore.getSnapshot()
      const towns = deriveTownsFromMarkers(snapshot.world?.markers || [])
      const sourceTown = findTownByName(towns, parsed.sourceTown)
      if (!sourceTown) return { applied: false, command, reason: 'Unknown town.' }
      const n = asPositiveIntegerAmount(parsed.n)
      if (n === null) return { applied: false, command, reason: 'Invalid n.' }

      let marketName = null
      if (parsed.marketName) {
        const market = findMarketByName(snapshot.world?.markets || [], parsed.marketName)
        if (!market) return { applied: false, command, reason: 'Unknown market.' }
        marketName = market.name
      }
      const rawReward = parsed.reward === null ? DEFAULT_TRADE_QUEST_REWARD : parsed.reward
      const baseReward = Number(rawReward)
      if (!Number.isInteger(baseReward) || baseReward < 0) return { applied: false, command, reason: 'Invalid reward.' }

      const tx = await memoryStore.transact((memory) => {
        const quests = ensureWorldQuests(memory.world)
        const clock = ensureWorldClock(memory.world)
        const at = now()
        const townName = sourceTown.townName
        const activeEvents = findActiveEventsForTown(memory.world, townName, clock.day)
        const rewardBonus = sumEventModifier(activeEvents, 'trade_reward_bonus')
        const reward = Math.max(0, baseReward + rewardBonus)
        const objective = { kind: 'trade_n', n }
        if (marketName) objective.market = marketName
        const flavor = buildQuestFlavor('trade_n', townName, objective)
        const questId = createQuestId(quests, operationId, townName, 'trade_n', at)
        const quest = {
          id: questId,
          type: 'trade_n',
          state: 'offered',
          town: townName,
          offered_at: new Date(at).toISOString(),
          objective,
          progress: { done: 0 },
          reward,
          title: flavor.title,
          desc: flavor.desc
        }
        quests.push(quest)
        const msg = `QUEST: offered ${quest.id} trade_n x${n}${marketName ? ` @ ${marketName}` : ''}`
        appendChronicle(memory, {
          id: `${operationId}:chronicle:quest_offer:${quest.id.toLowerCase()}`,
          type: 'quest_offer',
          msg,
          at,
          town: townName,
          meta: {
            quest_id: quest.id,
            quest_type: quest.type,
            reward,
            reward_bonus: rewardBonus
          }
        })
        appendNews(memory, {
          id: `${operationId}:news:quest_offer:${quest.id.toLowerCase()}`,
          topic: 'quest',
          msg,
          at,
          town: townName,
          meta: {
            quest_id: quest.id,
            quest_type: quest.type,
            reward,
            reward_bonus: rewardBonus
          }
        })
        return quest
      }, { eventId: `${operationId}:quest_offer:${sourceTown.townName.toLowerCase()}:trade_n` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [
          `GOD QUEST OFFERED: id=${tx.result.id} type=${tx.result.type} town=${tx.result.town || '-'} reward=${tx.result.reward} title=${tx.result.title}`
        ]
      }
    }

    if (parsed.type === 'quest_offer_visit_town') {
      const snapshot = memoryStore.getSnapshot()
      const towns = deriveTownsFromMarkers(snapshot.world?.markers || [])
      const sourceTown = findTownByName(towns, parsed.sourceTown)
      const targetTown = findTownByName(towns, parsed.targetTown)
      if (!sourceTown || !targetTown) return { applied: false, command, reason: 'Unknown town.' }
      const rawReward = parsed.reward === null ? DEFAULT_VISIT_QUEST_REWARD : parsed.reward
      const baseReward = Number(rawReward)
      if (!Number.isInteger(baseReward) || baseReward < 0) return { applied: false, command, reason: 'Invalid reward.' }

      const tx = await memoryStore.transact((memory) => {
        const quests = ensureWorldQuests(memory.world)
        const clock = ensureWorldClock(memory.world)
        const at = now()
        const activeEvents = findActiveEventsForTown(memory.world, sourceTown.townName, clock.day)
        const rewardBonus = sumEventModifier(activeEvents, 'visit_reward_bonus')
        const reward = Math.max(0, baseReward + rewardBonus)
        const objective = {
          kind: 'visit_town',
          town: targetTown.townName
        }
        const flavor = buildQuestFlavor('visit_town', sourceTown.townName, objective)
        const questId = createQuestId(quests, operationId, sourceTown.townName, 'visit_town', at)
        const quest = {
          id: questId,
          type: 'visit_town',
          state: 'offered',
          town: sourceTown.townName,
          offered_at: new Date(at).toISOString(),
          objective,
          progress: { visited: false },
          reward,
          title: flavor.title,
          desc: flavor.desc
        }
        quests.push(quest)
        const msg = `QUEST: offered ${quest.id} visit ${targetTown.townName}`
        appendChronicle(memory, {
          id: `${operationId}:chronicle:quest_offer:${quest.id.toLowerCase()}`,
          type: 'quest_offer',
          msg,
          at,
          town: sourceTown.townName,
          meta: {
            quest_id: quest.id,
            quest_type: quest.type,
            reward,
            reward_bonus: rewardBonus
          }
        })
        appendNews(memory, {
          id: `${operationId}:news:quest_offer:${quest.id.toLowerCase()}`,
          topic: 'quest',
          msg,
          at,
          town: sourceTown.townName,
          meta: {
            quest_id: quest.id,
            quest_type: quest.type,
            reward,
            reward_bonus: rewardBonus
          }
        })
        return quest
      }, { eventId: `${operationId}:quest_offer:${sourceTown.townName.toLowerCase()}:visit_town:${targetTown.townName.toLowerCase()}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [
          `GOD QUEST OFFERED: id=${tx.result.id} type=${tx.result.type} town=${tx.result.town || '-'} reward=${tx.result.reward} title=${tx.result.title}`
        ]
      }
    }

    if (parsed.type === 'quest_accept') {
      const snapshot = memoryStore.getSnapshot()
      const ownerName = resolveKnownAgentName(snapshot, runtimeAgents, parsed.agentName)
      if (!ownerName) return { applied: false, command, reason: 'Unknown agent.' }
      const existing = findQuestById(snapshot.world?.quests || [], parsed.questId)
      if (!existing) return { applied: false, command, reason: 'Unknown quest.' }
      if (existing.state !== 'offered') return { applied: false, command, reason: 'Quest is not in offered state.' }

      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const quests = ensureWorldQuests(memory.world)
          const idx = quests.findIndex(entry => asText(entry?.id, '', 200).toLowerCase() === existing.id.toLowerCase())
          if (idx < 0) {
            throw new AppError({
              code: 'UNKNOWN_QUEST',
              message: `Unknown quest: ${parsed.questId}`,
              recoverable: true
            })
          }
          const quest = normalizeQuest(quests[idx])
          if (!quest || quest.state !== 'offered') {
            throw new AppError({
              code: 'QUEST_STATE_INVALID',
              message: `Quest not offered: ${parsed.questId}`,
              recoverable: true
            })
          }
          const at = now()
          quest.owner = ownerName
          quest.accepted_at = new Date(at).toISOString()
          quest.state = 'accepted'
          quests[idx] = quest
          const town = asText(quest.town, '', 80) || undefined
          const msg = `QUEST: ${ownerName} accepted ${quest.id}`
          appendChronicle(memory, {
            id: `${operationId}:chronicle:quest_accept:${quest.id.toLowerCase()}`,
            type: 'quest_accept',
            msg,
            at,
            town,
            meta: {
              quest_id: quest.id,
              owner: ownerName
            }
          })
          appendNews(memory, {
            id: `${operationId}:news:quest_accept:${quest.id.toLowerCase()}`,
            topic: 'quest',
            msg,
            at,
            town,
            meta: {
              quest_id: quest.id,
              owner: ownerName
            }
          })
          return quest
        }, { eventId: `${operationId}:quest_accept:${existing.id.toLowerCase()}:${ownerName.toLowerCase()}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'UNKNOWN_QUEST') return { applied: false, command, reason: 'Unknown quest.' }
        if (err instanceof AppError && err.code === 'QUEST_STATE_INVALID') return { applied: false, command, reason: 'Quest is not in offered state.' }
        throw err
      }

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD QUEST ACCEPTED: id=${tx.result.id} owner=${tx.result.owner} state=${tx.result.state}`]
      }
    }

    if (parsed.type === 'quest_cancel') {
      const snapshot = memoryStore.getSnapshot()
      const existing = findQuestById(snapshot.world?.quests || [], parsed.questId)
      if (!existing) return { applied: false, command, reason: 'Unknown quest.' }
      if (!QUEST_CANCELABLE_STATES.has(existing.state)) return { applied: false, command, reason: 'Quest cannot be cancelled from current state.' }

      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const quests = ensureWorldQuests(memory.world)
          const idx = quests.findIndex(entry => asText(entry?.id, '', 200).toLowerCase() === existing.id.toLowerCase())
          if (idx < 0) {
            throw new AppError({
              code: 'UNKNOWN_QUEST',
              message: `Unknown quest: ${parsed.questId}`,
              recoverable: true
            })
          }
          const quest = normalizeQuest(quests[idx])
          if (!quest || !QUEST_CANCELABLE_STATES.has(quest.state)) {
            throw new AppError({
              code: 'QUEST_STATE_INVALID',
              message: `Quest not cancelable: ${parsed.questId}`,
              recoverable: true
            })
          }
          const at = now()
          quest.state = 'cancelled'
          quests[idx] = quest
          const town = asText(quest.town, '', 80) || undefined
          const msg = `QUEST: cancelled ${quest.id}`
          appendChronicle(memory, {
            id: `${operationId}:chronicle:quest_cancel:${quest.id.toLowerCase()}`,
            type: 'quest_cancel',
            msg,
            at,
            town,
            meta: { quest_id: quest.id }
          })
          appendNews(memory, {
            id: `${operationId}:news:quest_cancel:${quest.id.toLowerCase()}`,
            topic: 'quest',
            msg,
            at,
            town,
            meta: { quest_id: quest.id }
          })
          applyTownMoodDelta(memory, {
            townName: town || '-',
            delta: { unrest: 2 },
            at,
            idPrefix: `${operationId}:quest_cancel:${quest.id.toLowerCase()}`,
            reason: 'quest_cancel'
          })
          return quest
        }, { eventId: `${operationId}:quest_cancel:${existing.id.toLowerCase()}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'UNKNOWN_QUEST') return { applied: false, command, reason: 'Unknown quest.' }
        if (err instanceof AppError && err.code === 'QUEST_STATE_INVALID') return { applied: false, command, reason: 'Quest cannot be cancelled from current state.' }
        throw err
      }

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD QUEST CANCELED: id=${tx.result.id} state=${tx.result.state}`]
      }
    }

    if (parsed.type === 'quest_complete') {
      const snapshot = memoryStore.getSnapshot()
      const existing = findQuestById(snapshot.world?.quests || [], parsed.questId)
      if (!existing) return { applied: false, command, reason: 'Unknown quest.' }
      if (!QUEST_ACTIVE_STATES.has(existing.state)) return { applied: false, command, reason: 'Quest cannot complete from current state.' }
      if (!isQuestObjectiveSatisfied(existing)) return { applied: false, command, reason: 'Quest objective not satisfied.' }

      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const quests = ensureWorldQuests(memory.world)
          const idx = quests.findIndex(entry => asText(entry?.id, '', 200).toLowerCase() === existing.id.toLowerCase())
          if (idx < 0) {
            throw new AppError({
              code: 'UNKNOWN_QUEST',
              message: `Unknown quest: ${parsed.questId}`,
              recoverable: true
            })
          }
          const quest = normalizeQuest(quests[idx])
          if (!quest || !QUEST_ACTIVE_STATES.has(quest.state)) {
            throw new AppError({
              code: 'QUEST_STATE_INVALID',
              message: `Quest not completable: ${parsed.questId}`,
              recoverable: true
            })
          }
          if (!isQuestObjectiveSatisfied(quest)) {
            throw new AppError({
              code: 'QUEST_NOT_READY',
              message: `Quest objective not satisfied: ${parsed.questId}`,
              recoverable: true
            })
          }
          const completedQuest = completeQuestAndReward(
            quest,
            asText(quest.town, '', 80) || null,
            now(),
            `${operationId}:quest_complete`,
            memory
          )
          quests[idx] = completedQuest
          return completedQuest
        }, { eventId: `${operationId}:quest_complete:${existing.id.toLowerCase()}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'UNKNOWN_QUEST') return { applied: false, command, reason: 'Unknown quest.' }
        if (err instanceof AppError && err.code === 'QUEST_STATE_INVALID') return { applied: false, command, reason: 'Quest cannot complete from current state.' }
        if (err instanceof AppError && err.code === 'QUEST_NOT_READY') return { applied: false, command, reason: 'Quest objective not satisfied.' }
        if (err instanceof AppError && err.code === 'UNKNOWN_AGENT') return { applied: false, command, reason: 'Unknown agent.' }
        throw err
      }

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD QUEST COMPLETED: id=${tx.result.id} owner=${tx.result.owner || '-'} reward=${tx.result.reward}`]
      }
    }

    if (parsed.type === 'quest_visit') {
      const snapshot = memoryStore.getSnapshot()
      const existing = findQuestById(snapshot.world?.quests || [], parsed.questId)
      if (!existing) return { applied: false, command, reason: 'Unknown quest.' }
      const existingRumorTask = asText(existing.objective?.rumor_task, '', 20).toLowerCase()
      const visitCompatible = existing.type === 'visit_town'
        || (existing.type === 'rumor_task' && (existingRumorTask === 'rumor_visit' || existingRumorTask === 'rumor_choice'))
      if (!visitCompatible) return { applied: false, command, reason: 'Quest is not visit_town.' }
      if (!QUEST_ACTIVE_STATES.has(existing.state)) return { applied: false, command, reason: 'Quest cannot be visited from current state.' }
      if (!asText(existing.owner, '', 80)) return { applied: false, command, reason: 'Unknown agent.' }

      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const quests = ensureWorldQuests(memory.world)
          const idx = quests.findIndex(entry => asText(entry?.id, '', 200).toLowerCase() === existing.id.toLowerCase())
          if (idx < 0) {
            throw new AppError({
              code: 'UNKNOWN_QUEST',
              message: `Unknown quest: ${parsed.questId}`,
              recoverable: true
            })
          }
          const quest = normalizeQuest(quests[idx])
          const rumorTask = asText(quest?.objective?.rumor_task, '', 20).toLowerCase()
          const isVisitCompatible = quest
            && (quest.type === 'visit_town'
              || (quest.type === 'rumor_task' && (rumorTask === 'rumor_visit' || rumorTask === 'rumor_choice')))
          if (!isVisitCompatible) {
            throw new AppError({
              code: 'QUEST_TYPE_INVALID',
              message: `Quest is not visit_town: ${parsed.questId}`,
              recoverable: true
            })
          }
          if (!QUEST_ACTIVE_STATES.has(quest.state)) {
            throw new AppError({
              code: 'QUEST_STATE_INVALID',
              message: `Quest cannot be visited from state: ${quest.state}`,
              recoverable: true
            })
          }
          const ownerName = asText(quest.owner, '', 80)
          if (!ownerName) {
            throw new AppError({
              code: 'UNKNOWN_AGENT',
              message: 'Quest owner is missing.',
              recoverable: true
            })
          }
          quest.progress = { visited: true }
          const completedQuest = completeQuestAndReward(
            quest,
            asText(quest.town, '', 80) || null,
            now(),
            `${operationId}:quest_visit`,
            memory
          )
          quests[idx] = completedQuest
          return completedQuest
        }, { eventId: `${operationId}:quest_visit:${existing.id.toLowerCase()}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'UNKNOWN_QUEST') return { applied: false, command, reason: 'Unknown quest.' }
        if (err instanceof AppError && err.code === 'QUEST_TYPE_INVALID') return { applied: false, command, reason: 'Quest is not visit_town.' }
        if (err instanceof AppError && err.code === 'QUEST_STATE_INVALID') return { applied: false, command, reason: 'Quest cannot be visited from current state.' }
        if (err instanceof AppError && err.code === 'UNKNOWN_AGENT') return { applied: false, command, reason: 'Unknown agent.' }
        throw err
      }

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD QUEST VISIT: id=${tx.result.id} owner=${tx.result.owner || '-'} state=${tx.result.state} reward=${tx.result.reward}`]
      }
    }

    if (parsed.type === 'mark_list') {
      const snapshot = memoryStore.getSnapshot()
      const markers = (Array.isArray(snapshot.world?.markers) ? snapshot.world.markers : [])
        .map(normalizeMarker)
        .filter(Boolean)
      if (markers.length === 0) {
        return { applied: true, command, audit: false, outputLines: ['GOD MARK LIST: (none)'] }
      }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD MARK LIST: count=${markers.length}`,
          ...markers.map(marker => (
            `GOD MARK: ${marker.name} x=${marker.x} y=${marker.y} z=${marker.z} tag=${marker.tag || '(none)'} created_at=${marker.created_at}`
          ))
        ]
      }
    }

    if (parsed.type === 'mark_add') {
      const tx = await memoryStore.transact((memory) => {
        const markers = ensureWorldMarkers(memory.world)
        const at = now()
        const marker = {
          name: parsed.name,
          x: parsed.x,
          y: parsed.y,
          z: parsed.z,
          tag: parsed.tag || '',
          created_at: at
        }
        const idx = markers.findIndex(item => asText(item?.name, '', 80).toLowerCase() === parsed.name.toLowerCase())
        // Duplicate-name marker policy: OVERWRITE existing marker by name.
        if (idx >= 0) markers[idx] = marker
        else markers.push(marker)
        const town = parseTownNameFromTag(marker.tag)
        appendChronicle(memory, {
          id: `${operationId}:chronicle:marker_add:${parsed.name.toLowerCase()}`,
          type: 'marker_add',
          msg: `MARKER: ${parsed.name} x=${parsed.x} y=${parsed.y} z=${parsed.z}`,
          at,
          town: town || undefined,
          meta: {
            marker: parsed.name,
            tag: marker.tag || ''
          }
        })
        appendNews(memory, {
          id: `${operationId}:news:marker_add:${parsed.name.toLowerCase()}`,
          topic: 'marker',
          msg: `MARKER: added ${parsed.name}`,
          at,
          town: town || undefined,
          meta: {
            marker: parsed.name,
            tag: marker.tag || ''
          }
        })
        return marker
      }, { eventId: `${operationId}:mark_add:${parsed.name.toLowerCase()}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      // Persist marker state before optional runtime side effects to prevent drift.
      if (runtimeMark) await runtimeMark({ action: 'add', markerName: parsed.name, marker: tx.result })
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [
          `GOD MARK ADDED: ${parsed.name} x=${parsed.x} y=${parsed.y} z=${parsed.z} tag=${parsed.tag || '(none)'}`
        ]
      }
    }

    if (parsed.type === 'mark_remove') {
      const tx = await memoryStore.transact((memory) => {
        const markers = ensureWorldMarkers(memory.world)
        const idx = markers.findIndex(item => asText(item?.name, '', 80).toLowerCase() === parsed.name.toLowerCase())
        if (idx < 0) return { removed: false, name: parsed.name }
        const [removed] = markers.splice(idx, 1)
        const marker = normalizeMarker(removed) || { name: parsed.name, tag: '' }
        const town = parseTownNameFromTag(marker.tag)
        appendChronicle(memory, {
          id: `${operationId}:chronicle:marker_remove:${parsed.name.toLowerCase()}`,
          type: 'marker_remove',
          msg: `MARKER: removed ${marker.name}`,
          at: now(),
          town: town || undefined,
          meta: {
            marker: marker.name
          }
        })
        appendNews(memory, {
          id: `${operationId}:news:marker_remove:${parsed.name.toLowerCase()}`,
          topic: 'marker',
          msg: `MARKER: removed ${marker.name}`,
          at: now(),
          town: town || undefined,
          meta: {
            marker: marker.name
          }
        })
        return { removed: true, marker }
      }, { eventId: `${operationId}:mark_remove:${parsed.name.toLowerCase()}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      if (!tx.result?.removed) {
        return { applied: true, command, audit: true, outputLines: [`GOD MARK REMOVE: ${parsed.name} (not found)`] }
      }
      // Persist marker state before optional runtime side effects to prevent drift.
      if (runtimeMark) await runtimeMark({ action: 'remove', markerName: tx.result.marker.name, marker: tx.result.marker })
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD MARK REMOVED: ${tx.result.marker.name}`]
      }
    }

    if (parsed.type === 'job_roster') {
      const snapshot = memoryStore.getSnapshot()
      const rosterNames = new Map()
      for (const name of Object.keys(snapshot.agents || {})) {
        const safeName = asText(name, '', 80)
        if (!safeName) continue
        const key = safeName.toLowerCase()
        if (!rosterNames.has(key)) rosterNames.set(key, safeName)
      }
      for (const runtimeAgent of runtimeAgents) {
        const safeName = asText(runtimeAgent?.name, '', 80)
        if (!safeName) continue
        const key = safeName.toLowerCase()
        if (!rosterNames.has(key)) rosterNames.set(key, safeName)
      }
      const names = Array.from(rosterNames.values()).sort((a, b) => a.localeCompare(b))
      const rows = []
      for (const name of names) {
        const profile = snapshot.agents?.[name]?.profile
        const role = asText(profile?.job?.role, '', 20) || 'none'
        const homeMarker = asText(profile?.job?.home_marker, '', 80) || '-'
        const assignedAt = asText(profile?.job?.assigned_at, '', 80) || '-'
        rows.push(`GOD ROSTER ENTRY: ${name} role=${role} home_marker=${homeMarker} assigned_at=${assignedAt}`)
      }
      if (rows.length === 0) {
        return { applied: true, command, audit: false, outputLines: ['GOD ROSTER: (none)'] }
      }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [`GOD ROSTER: count=${rows.length}`, ...rows]
      }
    }

    if (parsed.type === 'job_set') {
      const runtimeAgent = resolveRuntimeAgent(runtimeAgents, parsed.name)
      if (!runtimeAgent) return { applied: false, command, reason: 'Unknown agent.' }
      if (!JOB_ROLES.has(parsed.role)) return { applied: false, command, reason: 'Invalid role.' }

      let homeMarkerName = null
      if (parsed.marker) {
        const snapshot = memoryStore.getSnapshot()
        const marker = findMarkerByName(snapshot.world?.markers || [], parsed.marker)
        if (!marker) return { applied: false, command, reason: `Unknown marker: ${parsed.marker}` }
        homeMarkerName = marker.name
      }

      const tx = await memoryStore.transact((memory) => {
        const profile = ensureAgentProfile(memory, runtimeAgent.name)
        const at = now()
        const job = {
          role: parsed.role,
          assigned_at: new Date(at).toISOString()
        }
        if (homeMarkerName) job.home_marker = homeMarkerName
        profile.job = job
        const town = findTownNameForMarker(memory.world?.markers || [], homeMarkerName)
        appendChronicle(memory, {
          id: `${operationId}:chronicle:job_set:${runtimeAgent.name.toLowerCase()}`,
          type: 'job_set',
          msg: `JOB: ${runtimeAgent.name} -> ${parsed.role}${homeMarkerName ? ` @ ${homeMarkerName}` : ''}`,
          at,
          town: town || undefined,
          meta: {
            agent: runtimeAgent.name,
            role: parsed.role,
            home_marker: homeMarkerName || ''
          }
        })
        appendNews(memory, {
          id: `${operationId}:news:job_set:${runtimeAgent.name.toLowerCase()}`,
          topic: 'job',
          msg: `JOB: ${runtimeAgent.name} -> ${parsed.role}${homeMarkerName ? ` @ ${homeMarkerName}` : ''}`,
          at,
          town: town || undefined,
          meta: {
            agent: runtimeAgent.name,
            role: parsed.role,
            home_marker: homeMarkerName || ''
          }
        })
        return job
      }, { eventId: `${operationId}:job_set:${runtimeAgent.name.toLowerCase()}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      // Persist job assignment before optional runtime side effects to prevent drift.
      if (runtimeJob) await runtimeJob({ action: 'set', agentName: runtimeAgent.name, job: tx.result || null })
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [
          `GOD JOB SET: ${runtimeAgent.name} role=${parsed.role}${homeMarkerName ? ` home_marker=${homeMarkerName}` : ''}`
        ]
      }
    }

    if (parsed.type === 'job_clear') {
      const runtimeAgent = resolveRuntimeAgent(runtimeAgents, parsed.name)
      if (!runtimeAgent) return { applied: false, command, reason: 'Unknown agent.' }

      const tx = await memoryStore.transact((memory) => {
        const profile = ensureAgentProfile(memory, runtimeAgent.name)
        const priorJob = (profile.job && typeof profile.job === 'object') ? profile.job : null
        const hadJob = !!(profile.job && typeof profile.job === 'object')
        delete profile.job
        const priorRole = asText(priorJob?.role, '', 20)
        const priorHome = asText(priorJob?.home_marker, '', 80)
        const town = findTownNameForMarker(memory.world?.markers || [], priorHome || null)
        appendChronicle(memory, {
          id: `${operationId}:chronicle:job_clear:${runtimeAgent.name.toLowerCase()}`,
          type: 'job_clear',
          msg: `JOB: cleared ${runtimeAgent.name}${priorRole ? ` (was ${priorRole})` : ''}`,
          at: now(),
          town: town || undefined,
          meta: {
            agent: runtimeAgent.name,
            role: priorRole || '',
            home_marker: priorHome || ''
          }
        })
        appendNews(memory, {
          id: `${operationId}:news:job_clear:${runtimeAgent.name.toLowerCase()}`,
          topic: 'job',
          msg: `JOB: cleared ${runtimeAgent.name}${priorRole ? ` (was ${priorRole})` : ''}`,
          at: now(),
          town: town || undefined,
          meta: {
            agent: runtimeAgent.name,
            role: priorRole || '',
            home_marker: priorHome || ''
          }
        })
        return { hadJob }
      }, { eventId: `${operationId}:job_clear:${runtimeAgent.name.toLowerCase()}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      // Persist job clear before optional runtime side effects to prevent drift.
      if (runtimeJob) await runtimeJob({ action: 'clear', agentName: runtimeAgent.name })
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [
          `GOD JOB CLEAR: ${runtimeAgent.name}${tx.result?.hadJob ? '' : ' (no existing job)'}`
        ]
      }
    }

    if (parsed.type === 'economy_mint') {
      const runtimeAgent = resolveRuntimeAgent(runtimeAgents, parsed.name)
      if (!runtimeAgent) return { applied: false, command, reason: 'Unknown agent.' }
      const amount = asPositiveIntegerAmount(parsed.amount)
      if (amount === null) return { applied: false, command, reason: 'Invalid amount.' }

      const tx = await memoryStore.transact((memory) => {
        const economy = ensureWorldEconomy(memory.world)
        const currentBalance = Number(economy.ledger[runtimeAgent.name] || 0)
        const nextBalance = currentBalance + amount
        economy.ledger[runtimeAgent.name] = nextBalance
        economy.minted_total = Number(economy.minted_total || 0) + amount
        return { agentName: runtimeAgent.name, amount, balance: nextBalance, currency: economy.currency }
      }, { eventId: `${operationId}:mint:${runtimeAgent.name.toLowerCase()}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [
          `GOD MINT: ${tx.result.agentName} +${tx.result.amount} ${tx.result.currency} balance=${tx.result.balance}`
        ]
      }
    }

    if (parsed.type === 'economy_transfer') {
      const fromAgent = resolveRuntimeAgent(runtimeAgents, parsed.from)
      const toAgent = resolveRuntimeAgent(runtimeAgents, parsed.to)
      if (!fromAgent || !toAgent) return { applied: false, command, reason: 'Unknown agent.' }
      const amount = asPositiveIntegerAmount(parsed.amount)
      if (amount === null) return { applied: false, command, reason: 'Invalid amount.' }

      const snapshot = memoryStore.getSnapshot()
      const economy = normalizeWorldEconomy(snapshot.world?.economy)
      const fromBalance = Number(economy.ledger[fromAgent.name] || 0)
      if (fromBalance < amount) return { applied: false, command, reason: 'Insufficient funds.' }

      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const worldEconomy = ensureWorldEconomy(memory.world)
          const fromCurrent = Number(worldEconomy.ledger[fromAgent.name] || 0)
          if (fromCurrent < amount) {
            throw new AppError({
              code: 'INSUFFICIENT_FUNDS',
              message: 'Insufficient funds for transfer.',
              recoverable: true
            })
          }
          const toCurrent = Number(worldEconomy.ledger[toAgent.name] || 0)
          const fromNext = fromCurrent - amount
          const toNext = toCurrent + amount
          worldEconomy.ledger[fromAgent.name] = fromNext
          worldEconomy.ledger[toAgent.name] = toNext
          return {
            fromAgentName: fromAgent.name,
            toAgentName: toAgent.name,
            amount,
            fromBalance: fromNext,
            toBalance: toNext,
            currency: worldEconomy.currency
          }
        }, { eventId: `${operationId}:transfer:${fromAgent.name.toLowerCase()}:${toAgent.name.toLowerCase()}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'INSUFFICIENT_FUNDS') {
          return { applied: false, command, reason: 'Insufficient funds.' }
        }
        throw err
      }

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [
          `GOD TRANSFER: ${tx.result.fromAgentName}->${tx.result.toAgentName} amount=${tx.result.amount} ${tx.result.currency} from_balance=${tx.result.fromBalance} to_balance=${tx.result.toBalance}`
        ]
      }
    }

    if (parsed.type === 'economy_balance') {
      const runtimeAgent = resolveRuntimeAgent(runtimeAgents, parsed.name)
      const agentName = runtimeAgent ? runtimeAgent.name : parsed.name
      const snapshot = memoryStore.getSnapshot()
      const economy = normalizeWorldEconomy(snapshot.world?.economy)
      const balance = Number(economy.ledger[agentName] || 0)
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [`GOD BALANCE: ${agentName} balance=${balance} currency=${economy.currency}`]
      }
    }

    if (parsed.type === 'economy_overview') {
      const snapshot = memoryStore.getSnapshot()
      const economy = normalizeWorldEconomy(snapshot.world?.economy)
      const entries = Object.entries(economy.ledger || {})
        .sort((a, b) => {
          const diff = Number(b[1]) - Number(a[1])
          if (diff !== 0) return diff
          return a[0].localeCompare(b[0])
        })
      const top = entries.slice(0, 5)
      const mintedTotal = Object.prototype.hasOwnProperty.call(economy, 'minted_total')
        ? economy.minted_total
        : '-'
      const lines = [
        `GOD ECONOMY: currency=${economy.currency} minted_total=${mintedTotal} accounts=${entries.length}`
      ]
      if (top.length === 0) {
        lines.push('GOD ECONOMY TOP: (none)')
      } else {
        top.forEach(([name, balance], idx) => {
          lines.push(`GOD ECONOMY TOP: rank=${idx + 1} agent=${name} balance=${Number(balance)}`)
        })
      }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: lines
      }
    }

    if (parsed.type === 'market_pulse_town') {
      const snapshot = memoryStore.getSnapshot()
      const townName = resolveTownName(snapshot.world, parsed.townName)
      if (!townName) return { applied: false, command, reason: 'Unknown town.' }
      const pulse = getMarketPulse(townName, snapshot.world)
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD MARKET PULSE: town=${townName}`,
          ...pulse.hot.map(item => `GOD MARKET PULSE HOT: good=${item.good} hint=${item.multiplierHint} reason=${item.reason}`),
          ...pulse.cold.map(item => `GOD MARKET PULSE COLD: good=${item.good} reason=${item.reason}`),
          `GOD MARKET PULSE RISK: label=${pulse.risk.label} reason=${pulse.risk.reason} note=${pulse.risk.nightPenaltyHint}`
        ]
      }
    }

    if (parsed.type === 'market_pulse_world') {
      const snapshot = memoryStore.getSnapshot()
      const towns = deriveTownNamesForEvents(snapshot.world)
      const clock = normalizeWorldClock(snapshot.world?.clock)
      if (towns.length === 0) {
        return {
          applied: true,
          command,
          audit: false,
          outputLines: ['GOD MARKET PULSE WORLD: (none)']
        }
      }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD MARKET PULSE WORLD: count=${towns.length} day=${clock.day} phase=${clock.phase} season=${clock.season}`,
          ...towns.map((townName) => {
            const pulse = getMarketPulse(townName, snapshot.world)
            const hotSummary = pulse.hot.map(item => item.good).slice(0, 2).join('|') || '-'
            const coldSummary = pulse.cold.map(item => item.good).slice(0, 2).join('|') || '-'
            return `GOD MARKET PULSE TOWN: town=${townName} hot=${hotSummary} cold=${coldSummary} risk=${pulse.risk.label}`
          })
        ]
      }
    }

    if (parsed.type === 'market_list') {
      const snapshot = memoryStore.getSnapshot()
      const markets = normalizeWorldMarkets(snapshot.world?.markets)
      if (markets.length === 0) {
        return { applied: true, command, audit: false, outputLines: ['GOD MARKET LIST: (none)'] }
      }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD MARKET LIST: count=${markets.length}`,
          ...markets.map(market => (
            `GOD MARKET: ${market.name} marker=${market.marker || '(none)'} offers=${market.offers.length} created_at=${market.created_at}`
          ))
        ]
      }
    }

    if (parsed.type === 'market_add') {
      const snapshot = memoryStore.getSnapshot()
      if (findMarketByName(snapshot.world?.markets || [], parsed.name)) {
        return { applied: false, command, reason: 'Market already exists.' }
      }

      let markerName = null
      if (parsed.marker) {
        const marker = findMarkerByName(snapshot.world?.markers || [], parsed.marker)
        if (!marker) return { applied: false, command, reason: `Unknown marker: ${parsed.marker}` }
        markerName = marker.name
      }

      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const markets = ensureWorldMarkets(memory.world)
          const existingIdx = markets.findIndex(item => asText(item?.name, '', 80).toLowerCase() === parsed.name.toLowerCase())
          if (existingIdx >= 0) {
            throw new AppError({
              code: 'MARKET_EXISTS',
              message: `Market already exists: ${parsed.name}`,
              recoverable: true
            })
          }
          const market = {
            name: parsed.name,
            created_at: now(),
            offers: []
          }
          if (markerName) market.marker = markerName
          markets.push(market)
          const town = findTownNameForMarker(memory.world?.markers || [], markerName)
          const message = `MARKET: opened ${parsed.name}${markerName ? ` @ ${markerName}` : ''}`
          appendChronicle(memory, {
            id: `${operationId}:chronicle:market_add:${parsed.name.toLowerCase()}`,
            type: 'market_add',
            msg: message,
            at: now(),
            town: town || undefined,
            meta: {
              market: parsed.name,
              marker: markerName || ''
            }
          })
          appendNews(memory, {
            id: `${operationId}:news:market_add:${parsed.name.toLowerCase()}`,
            topic: 'market',
            msg: message,
            at: now(),
            town: town || undefined,
            meta: {
              market: parsed.name,
              marker: markerName || ''
            }
          })
          return market
        }, { eventId: `${operationId}:market_add:${parsed.name.toLowerCase()}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'MARKET_EXISTS') {
          return { applied: false, command, reason: 'Market already exists.' }
        }
        throw err
      }

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD MARKET ADDED: ${parsed.name} marker=${markerName || '(none)'}`]
      }
    }

    if (parsed.type === 'market_remove') {
      const snapshot = memoryStore.getSnapshot()
      const existing = findMarketByName(snapshot.world?.markets || [], parsed.name)
      if (!existing) return { applied: false, command, reason: 'Unknown market.' }

      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const markets = ensureWorldMarkets(memory.world)
          const idx = markets.findIndex(item => asText(item?.name, '', 80).toLowerCase() === parsed.name.toLowerCase())
          if (idx < 0) {
            throw new AppError({
              code: 'UNKNOWN_MARKET',
              message: `Unknown market: ${parsed.name}`,
              recoverable: true
            })
          }
          const [removed] = markets.splice(idx, 1)
          const normalized = normalizeMarket(removed) || { name: parsed.name, marker: '', offers: [] }
          const town = findTownNameForMarker(memory.world?.markers || [], normalized.marker || null)
          const message = `MARKET: removed ${normalized.name}`
          appendChronicle(memory, {
            id: `${operationId}:chronicle:market_remove:${parsed.name.toLowerCase()}`,
            type: 'market_remove',
            msg: message,
            at: now(),
            town: town || undefined,
            meta: {
              market: normalized.name,
              marker: normalized.marker || ''
            }
          })
          appendNews(memory, {
            id: `${operationId}:news:market_remove:${parsed.name.toLowerCase()}`,
            topic: 'market',
            msg: message,
            at: now(),
            town: town || undefined,
            meta: {
              market: normalized.name,
              marker: normalized.marker || ''
            }
          })
          return normalized
        }, { eventId: `${operationId}:market_remove:${parsed.name.toLowerCase()}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'UNKNOWN_MARKET') {
          return { applied: false, command, reason: 'Unknown market.' }
        }
        throw err
      }

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD MARKET REMOVED: ${tx.result.name}`]
      }
    }

    if (parsed.type === 'offer_list') {
      const snapshot = memoryStore.getSnapshot()
      const market = findMarketByName(snapshot.world?.markets || [], parsed.marketName)
      if (!market) return { applied: false, command, reason: 'Unknown market.' }
      const offers = (Array.isArray(market.offers) ? market.offers : [])
        .map(normalizeOffer)
        .filter(Boolean)
      if (offers.length === 0) {
        return { applied: true, command, audit: false, outputLines: [`GOD OFFER LIST: market=${market.name} (none)`] }
      }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `GOD OFFER LIST: market=${market.name} count=${offers.length}`,
          ...offers.map(offer => (
            `GOD OFFER: id=${offer.offer_id} owner=${offer.owner} side=${offer.side} amount=${offer.amount} price=${offer.price} active=${offer.active} created_at=${offer.created_at}`
          ))
        ]
      }
    }

    if (parsed.type === 'offer_add') {
      const snapshot = memoryStore.getSnapshot()
      const ownerName = resolveKnownAgentName(snapshot, runtimeAgents, parsed.owner)
      if (!ownerName) return { applied: false, command, reason: 'Unknown agent.' }
      if (parsed.side !== 'buy' && parsed.side !== 'sell') {
        return { applied: false, command, reason: 'Invalid side.' }
      }
      const amount = asPositiveIntegerAmount(parsed.amount)
      if (amount === null) return { applied: false, command, reason: 'Invalid amount.' }
      const price = asPositiveIntegerAmount(parsed.price)
      if (price === null) return { applied: false, command, reason: 'Invalid price.' }
      const market = findMarketByName(snapshot.world?.markets || [], parsed.marketName)
      if (!market) return { applied: false, command, reason: 'Unknown market.' }

      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const markets = ensureWorldMarkets(memory.world)
          const marketIdx = markets.findIndex(item => asText(item?.name, '', 80).toLowerCase() === market.name.toLowerCase())
          if (marketIdx < 0) {
            throw new AppError({
              code: 'UNKNOWN_MARKET',
              message: `Unknown market: ${parsed.marketName}`,
              recoverable: true
            })
          }
          const marketRecord = markets[marketIdx]
          if (!Array.isArray(marketRecord.offers)) marketRecord.offers = []
          const baseOfferId = asText(
            `offer:${operationId}:${market.name.toLowerCase()}:${ownerName.toLowerCase()}`,
            `offer:${operationId}`.slice(0, 160),
            160
          )
          const usedIds = new Set(marketRecord.offers
            .map(entry => asText(entry?.offer_id, '', 160).toLowerCase())
            .filter(Boolean))
          const toOfferId = (idx) => {
            if (idx <= 1) return baseOfferId
            const suffix = `-${idx}`
            return `${baseOfferId.slice(0, Math.max(1, 160 - suffix.length))}${suffix}`
          }
          let counter = 1
          let offerId = toOfferId(counter)
          while (usedIds.has(offerId.toLowerCase())) {
            counter += 1
            offerId = toOfferId(counter)
          }

          const offer = {
            offer_id: offerId,
            owner: ownerName,
            side: parsed.side,
            amount,
            price,
            created_at: now(),
            active: true
          }
          marketRecord.offers.push(offer)
          const marketName = asText(marketRecord.name, market.name, 80)
          const markerName = asText(marketRecord.marker, '', 80) || null
          const town = findTownNameForMarker(memory.world?.markers || [], markerName)
          const message = `OFFER: ${offer.owner} ${offer.side} ${offer.amount} @ ${offer.price} in ${marketName}`
          appendChronicle(memory, {
            id: `${operationId}:chronicle:offer_add:${offer.offer_id.toLowerCase()}`,
            type: 'offer_add',
            msg: message,
            at: now(),
            town: town || undefined,
            meta: {
              market: marketName,
              offer_id: offer.offer_id,
              owner: offer.owner,
              side: offer.side,
              amount: offer.amount,
              price: offer.price
            }
          })
          appendNews(memory, {
            id: `${operationId}:news:offer_add:${offer.offer_id.toLowerCase()}`,
            topic: 'offer',
            msg: message,
            at: now(),
            town: town || undefined,
            meta: {
              market: marketName,
              offer_id: offer.offer_id,
              owner: offer.owner,
              side: offer.side,
              amount: offer.amount,
              price: offer.price
            }
          })
          return { marketName: asText(marketRecord.name, market.name, 80), offer }
        }, { eventId: `${operationId}:offer_add:${market.name.toLowerCase()}:${ownerName.toLowerCase()}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'UNKNOWN_MARKET') {
          return { applied: false, command, reason: 'Unknown market.' }
        }
        throw err
      }

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [
          `GOD OFFER ADDED: market=${tx.result.marketName} id=${tx.result.offer.offer_id} owner=${tx.result.offer.owner} side=${tx.result.offer.side} amount=${tx.result.offer.amount} price=${tx.result.offer.price}`
        ]
      }
    }

    if (parsed.type === 'offer_cancel') {
      const snapshot = memoryStore.getSnapshot()
      const market = findMarketByName(snapshot.world?.markets || [], parsed.marketName)
      if (!market) return { applied: false, command, reason: 'Unknown market.' }
      const existingOffer = findOfferById(market, parsed.offerId)
      if (!existingOffer || !existingOffer.active) {
        return { applied: false, command, reason: 'Offer missing or inactive.' }
      }

      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const markets = ensureWorldMarkets(memory.world)
          const marketIdx = markets.findIndex(item => asText(item?.name, '', 80).toLowerCase() === market.name.toLowerCase())
          if (marketIdx < 0) {
            throw new AppError({
              code: 'UNKNOWN_MARKET',
              message: `Unknown market: ${parsed.marketName}`,
              recoverable: true
            })
          }
          const marketRecord = markets[marketIdx]
          if (!Array.isArray(marketRecord.offers)) marketRecord.offers = []
          const offerIdx = marketRecord.offers.findIndex(item => asText(item?.offer_id, '', 160).toLowerCase() === existingOffer.offer_id.toLowerCase())
          if (offerIdx < 0) {
            throw new AppError({
              code: 'UNKNOWN_OFFER',
              message: `Unknown offer: ${parsed.offerId}`,
              recoverable: true
            })
          }
          const normalized = normalizeOffer(marketRecord.offers[offerIdx])
          if (!normalized || !normalized.active) {
            throw new AppError({
              code: 'UNKNOWN_OFFER',
              message: `Offer missing or inactive: ${parsed.offerId}`,
              recoverable: true
            })
          }
          marketRecord.offers[offerIdx] = { ...normalized, active: false }
          const marketName = asText(marketRecord.name, market.name, 80)
          const markerName = asText(marketRecord.marker, '', 80) || null
          const town = findTownNameForMarker(memory.world?.markers || [], markerName)
          const message = `OFFER: canceled ${normalized.offer_id} in ${marketName}`
          appendChronicle(memory, {
            id: `${operationId}:chronicle:offer_cancel:${normalized.offer_id.toLowerCase()}`,
            type: 'offer_cancel',
            msg: message,
            at: now(),
            town: town || undefined,
            meta: {
              market: marketName,
              offer_id: normalized.offer_id
            }
          })
          appendNews(memory, {
            id: `${operationId}:news:offer_cancel:${normalized.offer_id.toLowerCase()}`,
            topic: 'offer',
            msg: message,
            at: now(),
            town: town || undefined,
            meta: {
              market: marketName,
              offer_id: normalized.offer_id
            }
          })
          return {
            marketName,
            offerId: normalized.offer_id
          }
        }, { eventId: `${operationId}:offer_cancel:${market.name.toLowerCase()}:${existingOffer.offer_id.toLowerCase()}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'UNKNOWN_MARKET') {
          return { applied: false, command, reason: 'Unknown market.' }
        }
        if (err instanceof AppError && err.code === 'UNKNOWN_OFFER') {
          return { applied: false, command, reason: 'Offer missing or inactive.' }
        }
        throw err
      }

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`GOD OFFER CANCELED: market=${tx.result.marketName} id=${tx.result.offerId}`]
      }
    }

    if (parsed.type === 'market_trade') {
      const amount = asPositiveIntegerAmount(parsed.amount)
      if (amount === null) return { applied: false, command, reason: 'Invalid amount.' }

      const snapshot = memoryStore.getSnapshot()
      const buyerName = resolveKnownAgentName(snapshot, runtimeAgents, parsed.buyer)
      if (!buyerName) return { applied: false, command, reason: 'Unknown agent.' }
      const market = findMarketByName(snapshot.world?.markets || [], parsed.marketName)
      if (!market) return { applied: false, command, reason: 'Unknown market.' }
      const offer = findOfferById(market, parsed.offerId)
      if (!offer || !offer.active) return { applied: false, command, reason: 'Offer missing or inactive.' }
      if (amount > offer.amount) return { applied: false, command, reason: 'Trade amount exceeds remaining offer amount.' }

      const ownerName = resolveKnownAgentName(snapshot, runtimeAgents, offer.owner)
      if (!ownerName) return { applied: false, command, reason: 'Unknown agent.' }
      const payerName = offer.side === 'sell' ? buyerName : ownerName
      const payeeName = offer.side === 'sell' ? ownerName : buyerName
      const totalPrice = amount * offer.price
      if (!Number.isInteger(totalPrice) || !Number.isFinite(totalPrice) || totalPrice <= 0) {
        return { applied: false, command, reason: 'Invalid amount.' }
      }
      const snapshotEconomy = normalizeWorldEconomy(snapshot.world?.economy)
      if (Number(snapshotEconomy.ledger[payerName] || 0) < totalPrice) {
        return { applied: false, command, reason: 'Insufficient funds.' }
      }

      let tx
      try {
        tx = await memoryStore.transact((memory) => {
          const markets = ensureWorldMarkets(memory.world)
          const worldEconomy = ensureWorldEconomy(memory.world)
          const marketIdx = markets.findIndex(item => asText(item?.name, '', 80).toLowerCase() === market.name.toLowerCase())
          if (marketIdx < 0) {
            throw new AppError({
              code: 'UNKNOWN_MARKET',
              message: `Unknown market: ${parsed.marketName}`,
              recoverable: true
            })
          }
          const marketRecord = markets[marketIdx]
          if (!Array.isArray(marketRecord.offers)) marketRecord.offers = []
          const offerIdx = marketRecord.offers
            .findIndex(item => asText(item?.offer_id, '', 160).toLowerCase() === offer.offer_id.toLowerCase())
          if (offerIdx < 0) {
            throw new AppError({
              code: 'UNKNOWN_OFFER',
              message: `Unknown offer: ${parsed.offerId}`,
              recoverable: true
            })
          }
          const currentOffer = normalizeOffer(marketRecord.offers[offerIdx])
          if (!currentOffer || !currentOffer.active) {
            throw new AppError({
              code: 'UNKNOWN_OFFER',
              message: `Offer missing or inactive: ${parsed.offerId}`,
              recoverable: true
            })
          }
          if (amount > currentOffer.amount) {
            throw new AppError({
              code: 'TRADE_AMOUNT_EXCEEDS',
              message: `Trade amount exceeds offer amount: ${amount} > ${currentOffer.amount}`,
              recoverable: true
            })
          }
          const buyerCanonical = resolveKnownAgentName(memory, runtimeAgents, buyerName)
          const ownerCanonical = resolveKnownAgentName(memory, runtimeAgents, currentOffer.owner)
          if (!buyerCanonical || !ownerCanonical) {
            throw new AppError({
              code: 'UNKNOWN_AGENT',
              message: 'Unknown trade agent.',
              recoverable: true
            })
          }
          if (currentOffer.side !== 'buy' && currentOffer.side !== 'sell') {
            throw new AppError({
              code: 'INVALID_OFFER_SIDE',
              message: `Invalid offer side: ${currentOffer.side}`,
              recoverable: true
            })
          }
          const unitPrice = asPositiveIntegerAmount(currentOffer.price)
          if (unitPrice === null) {
            throw new AppError({
              code: 'INVALID_AMOUNT',
              message: `Invalid offer price: ${currentOffer.price}`,
              recoverable: true
            })
          }
          const payer = currentOffer.side === 'sell' ? buyerCanonical : ownerCanonical
          const payee = currentOffer.side === 'sell' ? ownerCanonical : buyerCanonical
          const total = amount * unitPrice
          if (!Number.isInteger(total) || !Number.isFinite(total) || total <= 0) {
            throw new AppError({
              code: 'INVALID_AMOUNT',
              message: 'Invalid trade total.',
              recoverable: true
            })
          }
          const payerCurrent = Number(worldEconomy.ledger[payer] || 0)
          if (payerCurrent < total) {
            throw new AppError({
              code: 'INSUFFICIENT_FUNDS',
              message: 'Insufficient funds for trade.',
              recoverable: true
            })
          }
          const payeeCurrent = Number(worldEconomy.ledger[payee] || 0)
          worldEconomy.ledger[payer] = payerCurrent - total
          worldEconomy.ledger[payee] = payeeCurrent + total
          const remaining = currentOffer.amount - amount
          const nextOffer = { ...currentOffer, amount: remaining, active: remaining > 0 }
          marketRecord.offers[offerIdx] = nextOffer
          const marketName = asText(marketRecord.name, market.name, 80)
          const town = findTownNameForMarker(memory.world?.markers || [], asText(marketRecord.marker, '', 80) || null)
          const tradeMessage = `TRADE: ${buyerCanonical} bought ${amount} @ ${unitPrice} from ${ownerCanonical} at ${marketName}`
          const tradeAt = now()
          appendChronicle(memory, {
            id: `${operationId}:chronicle:trade:${currentOffer.offer_id.toLowerCase()}`,
            type: 'trade',
            msg: tradeMessage,
            at: tradeAt,
            town: town || undefined,
            meta: {
              market: marketName,
              offer_id: currentOffer.offer_id,
              side: currentOffer.side,
              amount,
              price: unitPrice,
              total
            }
          })
          appendNews(memory, {
            id: `${operationId}:news:trade:${currentOffer.offer_id.toLowerCase()}`,
            topic: 'trade',
            msg: tradeMessage,
            at: tradeAt,
            town: town || undefined,
            meta: {
              market: marketName,
              offer_id: currentOffer.offer_id,
              side: currentOffer.side,
              amount,
              price: unitPrice,
              total
            }
          })
          applyTownMoodDelta(memory, {
            townName: town || '-',
            delta: { prosperity: 1, unrest: -1 },
            at: tradeAt,
            idPrefix: `${operationId}:trade:${currentOffer.offer_id.toLowerCase()}`,
            reason: 'trade'
          })
          const questCompletions = []
          const quests = ensureWorldQuests(memory.world)
          for (let questIdx = 0; questIdx < quests.length; questIdx += 1) {
            const quest = normalizeQuest(quests[questIdx])
            if (!quest) continue
            const rumorTask = asText(quest.objective?.rumor_task, '', 20).toLowerCase()
            const isTradeQuest = quest.type === 'trade_n'
              || (quest.type === 'rumor_task' && rumorTask === 'rumor_trade')
            if (!isTradeQuest) continue
            if (!QUEST_ACTIVE_STATES.has(quest.state)) continue
            if (!sameText(quest.owner, buyerCanonical, 80)) continue
            const objectiveMarket = asText(quest.objective.market, '', 80)
            if (objectiveMarket && !sameText(objectiveMarket, marketName, 80)) continue

            const nextDone = Number(quest.progress.done || 0) + 1
            quest.progress = { done: nextDone }
            if (quest.state === 'accepted') quest.state = 'in_progress'
            if (nextDone >= Number(quest.objective.n || 0)) {
              const completedQuest = completeQuestAndReward(
                quest,
                asText(quest.town, '', 80) || town || null,
                now(),
                `${operationId}:trade_quest:${currentOffer.offer_id.toLowerCase()}`,
                memory
              )
              quests[questIdx] = completedQuest
              questCompletions.push(completedQuest.id)
              continue
            }
            quests[questIdx] = quest
          }
          return {
            marketName,
            offerId: currentOffer.offer_id,
            side: currentOffer.side,
            buyerName: buyerCanonical,
            ownerName: ownerCanonical,
            amount,
            price: unitPrice,
            totalPrice: total,
            payerName: payer,
            payeeName: payee,
            payerBalance: worldEconomy.ledger[payer],
            payeeBalance: worldEconomy.ledger[payee],
            remaining,
            active: nextOffer.active,
            questCompletions
          }
        }, { eventId: `${operationId}:trade:${market.name.toLowerCase()}:${offer.offer_id.toLowerCase()}:${buyerName.toLowerCase()}:${amount}` })
      } catch (err) {
        if (err instanceof AppError && err.code === 'UNKNOWN_MARKET') {
          return { applied: false, command, reason: 'Unknown market.' }
        }
        if (err instanceof AppError && err.code === 'UNKNOWN_OFFER') {
          return { applied: false, command, reason: 'Offer missing or inactive.' }
        }
        if (err instanceof AppError && err.code === 'UNKNOWN_AGENT') {
          return { applied: false, command, reason: 'Unknown agent.' }
        }
        if (err instanceof AppError && err.code === 'TRADE_AMOUNT_EXCEEDS') {
          return { applied: false, command, reason: 'Trade amount exceeds remaining offer amount.' }
        }
        if (err instanceof AppError && err.code === 'INSUFFICIENT_FUNDS') {
          return { applied: false, command, reason: 'Insufficient funds.' }
        }
        if (err instanceof AppError && err.code === 'INVALID_AMOUNT') {
          return { applied: false, command, reason: 'Invalid amount.' }
        }
        if (err instanceof AppError && err.code === 'INVALID_OFFER_SIDE') {
          return { applied: false, command, reason: 'Invalid side.' }
        }
        throw err
      }

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [
          `GOD TRADE: market=${tx.result.marketName} offer_id=${tx.result.offerId} side=${tx.result.side} buyer=${tx.result.buyerName} amount=${tx.result.amount} unit_price=${tx.result.price} total=${tx.result.totalPrice} payer=${tx.result.payerName} payee=${tx.result.payeeName} payer_balance=${tx.result.payerBalance} payee_balance=${tx.result.payeeBalance} remaining=${tx.result.remaining} active=${tx.result.active}`
        ]
      }
    }

    if (parsed.type === 'leader_set') {
      const runtimeAgent = resolveRuntimeAgent(runtimeAgents, parsed.name)
      if (!runtimeAgent) {
        throw new AppError({
          code: 'UNKNOWN_AGENT',
          message: `Unknown agent for leader set: ${parsed.name}`,
          recoverable: true
        })
      }
      const leaderName = runtimeAgent.name
      const tx = await memoryStore.transact((memory) => {
        for (const [name, record] of Object.entries(memory.agents || {})) {
          const profile = ensureAgentProfile(memory, name)
          const worldIntent = normalizeWorldIntent(profile)
          worldIntent.is_leader = false
          profile.world_intent = worldIntent
          if (record && typeof record === 'object') {
            memory.agents[name] = { ...record, profile }
          }
        }
        const profile = ensureAgentProfile(memory, leaderName)
        const worldIntent = normalizeWorldIntent(profile)
        worldIntent.is_leader = true
        worldIntent.last_action = 'leader_set'
        worldIntent.last_action_at = now()
        profile.world_intent = worldIntent
      }, { eventId: `${operationId}:leader_set:${leaderName.toLowerCase()}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      runtimeAgents.forEach(agent => {
        agent.worldLeader = leaderName
      })
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`LEADER SET: ${leaderName}`]
      }
    }

    if (parsed.type === 'leader_clear') {
      const tx = await memoryStore.transact((memory) => {
        for (const name of Object.keys(memory.agents || {})) {
          const profile = ensureAgentProfile(memory, name)
          const worldIntent = normalizeWorldIntent(profile)
          worldIntent.is_leader = false
          worldIntent.last_action = 'leader_cleared'
          worldIntent.last_action_at = now()
          profile.world_intent = worldIntent
        }
      }, { eventId: `${operationId}:leader_clear` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      runtimeAgents.forEach(agent => {
        agent.worldLeader = null
      })
      return {
        applied: true,
        command,
        audit: true,
        outputLines: ['LEADER CLEARED']
      }
    }

    if (parsed.type === 'freeze' || parsed.type === 'unfreeze') {
      const runtimeAgent = resolveRuntimeAgent(runtimeAgents, parsed.name)
      if (!runtimeAgent) {
        throw new AppError({
          code: 'UNKNOWN_AGENT',
          message: `Unknown agent: ${parsed.name}`,
          recoverable: true
        })
      }
      const freeze = parsed.type === 'freeze'
      const tx = await memoryStore.transact((memory) => {
        const profile = ensureAgentProfile(memory, runtimeAgent.name)
        const worldIntent = normalizeWorldIntent(profile)
        worldIntent.frozen = freeze
        if (freeze) {
          worldIntent.intent = 'idle'
          worldIntent.intent_target = null
        }
        worldIntent.last_action = freeze ? 'freeze' : 'unfreeze'
        worldIntent.last_action_at = now()
        profile.world_intent = worldIntent
      }, { eventId: `${operationId}:${freeze ? 'freeze' : 'unfreeze'}:${runtimeAgent.name.toLowerCase()}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      runtimeAgent.worldFrozen = freeze
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`${freeze ? 'FROZEN' : 'UNFROZEN'}: ${runtimeAgent.name}`]
      }
    }

    if (parsed.type === 'intent_set') {
      const runtimeAgent = resolveRuntimeAgent(runtimeAgents, parsed.name)
      if (!runtimeAgent) {
        throw new AppError({
          code: 'UNKNOWN_AGENT',
          message: `Unknown agent: ${parsed.name}`,
          recoverable: true
        })
      }
      const tx = await memoryStore.transact((memory) => {
        const profile = ensureAgentProfile(memory, runtimeAgent.name)
        const worldIntent = normalizeWorldIntent(profile)
        const fallbackLeader = findCurrentLeader(memory)
        const target = parsed.intent === 'follow'
          ? (asText(parsed.target, '', 80) || fallbackLeader || null)
          : null
        worldIntent.intent = parsed.intent
        worldIntent.intent_target = target
        worldIntent.intent_set_at = now()
        worldIntent.manual_override = true
        worldIntent.last_action = 'intent_set'
        worldIntent.last_action_at = now()
        profile.world_intent = worldIntent
      }, { eventId: `${operationId}:intent_set:${runtimeAgent.name.toLowerCase()}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [
          `INTENT SET: ${runtimeAgent.name} -> ${parsed.intent}${parsed.target ? ` (${parsed.target})` : ''}`
        ]
      }
    }

    if (parsed.type === 'say') {
      const runtimeAgent = resolveRuntimeAgent(runtimeAgents, parsed.name)
      if (!runtimeAgent) {
        throw new AppError({
          code: 'UNKNOWN_AGENT',
          message: `Unknown agent: ${parsed.name}`,
          recoverable: true
        })
      }
      if (!runtimeSay) {
        logger.info('god_say_no_runtime_hook', { agent: runtimeAgent.name })
      } else {
        await runtimeSay({ agent: runtimeAgent, message: parsed.message })
      }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [`GOD SAY: ${runtimeAgent.name} <- ${parsed.message}`]
      }
    }

    throw new AppError({
      code: 'INVALID_GOD_COMMAND',
      message: `Unsupported god command: ${command || '(empty)'}`,
      recoverable: true
    })
  }

  return {
    applyGodCommand,
    SUPPORTED_GOD_COMMANDS
  }
}

module.exports = { createGodCommandService, SUPPORTED_GOD_COMMANDS, parseGodCommand }
