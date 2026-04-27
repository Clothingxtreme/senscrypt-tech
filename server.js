const express = require("express")
const http = require("http")
const mongoose = require("mongoose")
const cors = require("cors")
const axios = require("axios")
const crypto = require("crypto")
const path = require("path")
const { Server } = require("socket.io")
const { AxiosError } = require("axios")

require("dotenv").config({ path: path.join(__dirname, ".env") })

function readEnv(key) {
  let value = String(process.env[key] || "").trim()

  if (
    (value.startsWith('"') && value.endsWith('"')) ||
    (value.startsWith("'") && value.endsWith("'"))
  ) {
    value = value.slice(1, -1).trim()
  }

  if (value.startsWith(`${key}=`)) {
    value = value.slice(key.length + 1).trim()
  }

  return value
}

const MONGODB_URI = readEnv("MONGODB_URI")
const MONNIFY_API_KEY = readEnv("MONNIFY_API_KEY")
const MONNIFY_SECRET_KEY = readEnv("MONNIFY_SECRET_KEY")
const MONNIFY_CONTRACT_CODE = readEnv("MONNIFY_CONTRACT_CODE")
const MONNIFY_BASE_URL = readEnv("MONNIFY_BASE_URL")
const FRONTEND_ORIGIN = readEnv("FRONTEND_ORIGIN")
const MONNIFY_WEBHOOK_IP_ALLOWLIST = readEnv("MONNIFY_WEBHOOK_IP_ALLOWLIST")
const GOOGLE_CLIENT_ID = readEnv("GOOGLE_CLIENT_ID")
const APPLE_CLIENT_ID = readEnv("APPLE_CLIENT_ID")
const ADMIN_EMAIL = readEnv("ADMIN_EMAIL")
const ADMIN_PASSWORD = readEnv("ADMIN_PASSWORD")
const MONNIFY_DISBURSEMENT_SOURCE_ACCOUNT_NUMBER = readEnv(
  "MONNIFY_DISBURSEMENT_SOURCE_ACCOUNT_NUMBER",
)

const missingRequiredEnv = ["MONGODB_URI"].filter((key) => !process.env[key])
if (missingRequiredEnv.length > 0) {
  console.error(`Missing required environment variables: ${missingRequiredEnv.join(", ")}`)
}

function parseAllowedOrigins(value) {
  const origins = String(value || "")
    .split(",")
    .map((origin) => origin.trim())
    .filter(Boolean)

  if (origins.length === 0 || origins.includes("*")) {
    return "*"
  }

  return origins
}

function parseCsvList(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)
}

const allowedOrigins = parseAllowedOrigins(FRONTEND_ORIGIN)
const monnifyWebhookIpAllowlist = parseCsvList(MONNIFY_WEBHOOK_IP_ALLOWLIST)

function isOriginAllowed(origin) {
  if (!origin || allowedOrigins === "*") {
    return true
  }

  return allowedOrigins.some((allowedOrigin) => {
    if (allowedOrigin === origin) {
      return true
    }

    if (!allowedOrigin.includes("*")) {
      return false
    }

    const escapedPattern = allowedOrigin
      .replace(/[.+?^${}()|[\]\\]/g, "\\$&")
      .replace(/\*/g, ".*")
    const regex = new RegExp(`^${escapedPattern}$`)

    return regex.test(origin)
  })
}

function normalizeIpAddress(value) {
  return String(value || "").replace(/^::ffff:/, "").trim()
}

function getRequestIpAddresses(req) {
  const forwardedFor = String(req.headers["x-forwarded-for"] || "")
    .split(",")
    .map((item) => normalizeIpAddress(item))
    .filter(Boolean)

  const directIps = [
    normalizeIpAddress(req.ip),
    normalizeIpAddress(req.socket?.remoteAddress),
  ].filter(Boolean)

  return Array.from(new Set([...forwardedFor, ...directIps]))
}

function isWebhookIpAllowed(req) {
  if (monnifyWebhookIpAllowlist.length === 0) {
    return true
  }

  const requestIps = getRequestIpAddresses(req)
  return requestIps.some((ip) => monnifyWebhookIpAllowlist.includes(ip))
}

const app = express()
app.use(
  cors({
    origin(origin, callback) {
      if (isOriginAllowed(origin)) {
        return callback(null, true)
      }

      return callback(new Error("Origin not allowed by CORS."))
    },
  }),
)
app.use(
  express.json({
    verify: (req, _res, buffer) => {
      req.rawBody = buffer.toString("utf8")
    },
  }),
)

const server = http.createServer(app)
const io = new Server(server, {
  cors: {
    origin: allowedOrigins === "*" ? true : allowedOrigins,
  },
})

const PLATFORM_FEE_RATE = 0.2
const CREATOR_SHARE_RATE = 0.8
let lastDatabaseError = ""

const defaultOverlaySettings = {
  animations: true,
  sounds: true,
  notifications: false,
  preview: false,
}

const defaultOverlayCustomization = {
  giftPack: "tiktok",
  goalAmount: 250000,
  goalTitle: "Tonight's Gift Goal",
  previewPlatform: "tiktok",
  goalOpacity: 0.92,
  leaderboardOpacity: 0.88,
  accountOpacity: 0.9,
  showLeaderboard: true,
  showGoal: true,
  showAccountDetails: true,
  showTopSupporter: true,
  showAmbientScene: true,
  alertPosition: { anchor: "center", offsetX: 0, offsetY: 12 },
  goalPosition: { anchor: "top-center", offsetX: 0, offsetY: 0 },
  leaderboardPosition: { anchor: "top-right", offsetX: 0, offsetY: 0 },
  accountPosition: { anchor: "bottom-left", offsetX: 0, offsetY: 0 },
  goalGradient: { start: "#22d3ee", middle: "#8b5cf6", end: "#ec4899" },
  leaderboardGradient: { start: "#818cf8", middle: "#22d3ee", end: "#f472b6" },
  accountGradient: { start: "#1d4ed8", middle: "#0f766e", end: "#22c55e" },
  alertGradient: { start: "#f472b6", middle: "#8b5cf6", end: "#38bdf8" },
}

const defaultCustomGifts = [
  { id: "gift-rose", name: "Rose", icon: "Rose", minAmount: 1, maxAmount: 49, animationType: "rose-bloom" },
  { id: "gift-panda", name: "Panda", icon: "Panda", minAmount: 50, maxAmount: 299, animationType: "bubble-pop" },
  { id: "gift-perfume", name: "Perfume", icon: "Perfume", minAmount: 300, maxAmount: 999, animationType: "petal-rain" },
  { id: "gift-confetti", name: "Confetti", icon: "Confetti", minAmount: 1000, maxAmount: 4999, animationType: "confetti-burst" },
  { id: "gift-money-rain", name: "Money Rain", icon: "Money Rain", minAmount: 5000, maxAmount: 14999, animationType: "money-cannon" },
  { id: "gift-disco-ball", name: "Disco Ball", icon: "Disco Ball", minAmount: 15000, maxAmount: 44999, animationType: "disco-spin" },
  { id: "gift-airplane", name: "Airplane", icon: "Airplane", minAmount: 45000, maxAmount: 149999, animationType: "airplane-flyover" },
  { id: "gift-lion", name: "Lion", icon: "Lion", minAmount: 150000, maxAmount: 449999, animationType: "lion-roar" },
  { id: "gift-universe", name: "Universe", icon: "Universe", minAmount: 450000, maxAmount: 2000000, animationType: "universe-rift" },
]

function getMongoUriHost() {
  if (!MONGODB_URI) {
    return ""
  }

  try {
    return new URL(MONGODB_URI).host
  } catch (_error) {
    return "invalid-uri"
  }
}

if (MONGODB_URI) {
  mongoose
    .connect(MONGODB_URI, { serverSelectionTimeoutMS: 15000 })
    .then(() => console.log("MongoDB Connected"))
    .catch((error) => {
      lastDatabaseError = error instanceof Error ? error.message : String(error)
      console.error("MongoDB connection failed", lastDatabaseError)
    })
} else {
  lastDatabaseError = "MONGODB_URI is missing."
}

function isDatabaseConnected() {
  return mongoose.connection.readyState === 1
}

function requireDatabaseReady(_req, res, next) {
  if (isDatabaseConnected()) {
    return next()
  }

  return res.status(503).json({
    error: "Database is not connected. Check MONGODB_URI in the backend environment variables.",
  })
}

const donationSchema = new mongoose.Schema({
  creatorId: { type: mongoose.Schema.Types.ObjectId, ref: "User", index: true },
  creatorEmail: String,
  sender: String,
  senderNameSource: String,
  amount: Number,
  platformFee: Number,
  creatorShare: Number,
  eventType: String,
  paymentStatus: String,
  destinationAccountNumber: String,
  destinationBankName: String,
  monnifyTransactionReference: { type: String, index: true, sparse: true },
  monnifyPaymentReference: { type: String, index: true, sparse: true },
  date: { type: Date, default: Date.now },
})

const Donation = mongoose.model("Donation", donationSchema)

const payoutSchema = new mongoose.Schema({
  creatorId: { type: mongoose.Schema.Types.ObjectId, ref: "User", index: true },
  amount: Number,
  status: String,
  bankName: String,
  bankCode: String,
  accountNumber: String,
  accountName: String,
  transferReference: String,
  providerReference: String,
  providerMessage: String,
  createdAt: { type: Date, default: Date.now },
  completedAt: Date,
})

const Payout = mongoose.model("Payout", payoutSchema)

const platformWithdrawalSchema = new mongoose.Schema({
  amount: Number,
  status: String,
  bankName: String,
  bankCode: String,
  accountNumber: String,
  accountName: String,
  note: String,
  transferReference: String,
  providerReference: String,
  providerMessage: String,
  createdAt: { type: Date, default: Date.now },
  completedAt: Date,
})

const PlatformWithdrawal = mongoose.model("PlatformWithdrawal", platformWithdrawalSchema)

const adminSessionSchema = new mongoose.Schema(
  {
    token: { type: String, required: true, unique: true },
    createdAt: { type: Date, default: Date.now },
    lastSeenAt: { type: Date, default: Date.now },
  },
  { timestamps: true },
)

const auditLogSchema = new mongoose.Schema(
  {
    actorType: { type: String, required: true },
    actorId: { type: String, default: "" },
    eventType: { type: String, required: true },
    message: { type: String, required: true },
    metadata: { type: Object, default: {} },
    createdAt: { type: Date, default: Date.now },
  },
  { timestamps: false },
)

const AdminSession = mongoose.model("AdminSession", adminSessionSchema)
const AuditLog = mongoose.model("AuditLog", auditLogSchema)

const userSchema = new mongoose.Schema(
  {
    name: { type: String, required: true },
    email: { type: String, required: true, unique: true, lowercase: true, trim: true },
    passwordHash: { type: String, required: true },
    sessionToken: { type: String, default: null },
    googleSub: { type: String, default: "", index: true },
    appleSub: { type: String, default: "", index: true },
    role: { type: String, enum: ["creator", "admin"], default: "creator" },
    status: { type: String, enum: ["active", "suspended", "banned"], default: "active" },
    profileImage: { type: String, default: "" },
    identity: {
      bvn: { type: String, default: "" },
      nin: { type: String, default: "" },
    },
    virtualAccount: {
      accountReference: String,
      accountName: String,
      accountNumber: String,
      bankName: String,
      bankCode: String,
      reservationReference: String,
      status: { type: String, default: "inactive" },
      provider: { type: String, default: "monnify" },
      environment: { type: String, enum: ["sandbox", "live"], default: "sandbox" },
      createdAt: Date,
    },
    overlayState: {
      settings: { type: mongoose.Schema.Types.Mixed, default: defaultOverlaySettings },
      customization: { type: mongoose.Schema.Types.Mixed, default: defaultOverlayCustomization },
      customGifts: { type: mongoose.Schema.Types.Mixed, default: defaultCustomGifts },
      leaderboardResetAt: Date,
      updatedAt: Date,
    },
  },
  {
    timestamps: true,
  },
)

const User = mongoose.model("User", userSchema)

function getCreatorRoom(userId) {
  return `creator:${String(userId)}`
}

const fallbackBanks = [
  { name: "Access Bank", code: "044" },
  { name: "First Bank of Nigeria", code: "011" },
  { name: "Fidelity Bank", code: "070" },
  { name: "FCMB", code: "214" },
  { name: "Guaranty Trust Bank", code: "058" },
  { name: "Jaiz Bank", code: "301" },
  { name: "Keystone Bank", code: "082" },
  { name: "Kuda Bank", code: "50211" },
  { name: "Moniepoint MFB", code: "50515" },
  { name: "OPay", code: "999992" },
  { name: "Polaris Bank", code: "076" },
  { name: "Providus Bank", code: "101" },
  { name: "Stanbic IBTC Bank", code: "221" },
  { name: "Sterling Bank", code: "232" },
  { name: "UBA", code: "033" },
  { name: "Union Bank", code: "032" },
  { name: "Unity Bank", code: "215" },
  { name: "Wema Bank", code: "035" },
  { name: "Zenith Bank", code: "057" },
]

function isMonnifyConfigured() {
  return Boolean(MONNIFY_API_KEY && MONNIFY_SECRET_KEY && MONNIFY_CONTRACT_CODE && MONNIFY_BASE_URL)
}

function isMonnifySandbox() {
  return String(MONNIFY_BASE_URL || "").toLowerCase().includes("sandbox.monnify.com")
}

function getMonnifyEnvironment() {
  if (!MONNIFY_BASE_URL) {
    return "unconfigured"
  }

  return isMonnifySandbox() ? "sandbox" : "live"
}

function requiresMonnifyCustomerVerification() {
  return getMonnifyEnvironment() === "live"
}

function sanitizeIdentity(identity) {
  const bvn = String(identity?.bvn || "").trim()
  const nin = String(identity?.nin || "").trim()

  return {
    hasBvn: Boolean(bvn),
    hasNin: Boolean(nin),
    bvnLast4: bvn ? bvn.slice(-4) : "",
    ninLast4: nin ? nin.slice(-4) : "",
  }
}

function getOverlaySlug(user) {
  const slug = String(user?.name || "creator")
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")

  return slug || "creator"
}

function sanitizePlainObject(value, fallback) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return { ...fallback }
  }

  return {
    ...fallback,
    ...value,
  }
}

function getOverlayStateForUser(user) {
  const overlayState = user?.overlayState || {}

  return {
    settings: sanitizePlainObject(overlayState.settings, defaultOverlaySettings),
    customization: sanitizePlainObject(overlayState.customization, defaultOverlayCustomization),
    customGifts: Array.isArray(overlayState.customGifts) && overlayState.customGifts.length
      ? overlayState.customGifts
      : defaultCustomGifts,
    leaderboardResetAt: overlayState.leaderboardResetAt
      ? new Date(overlayState.leaderboardResetAt).toISOString()
      : "",
  }
}

function normalizeName(value) {
  return String(value || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim()
}

function getNestedValue(source, path) {
  return path.split(".").reduce((current, key) => {
    if (Array.isArray(current)) {
      current = current[0]
    }

    if (!current || typeof current !== "object") {
      return undefined
    }

    return current[key]
  }, source)
}

function sanitizeDonorDisplayName(value) {
  return String(value || "")
    .replace(/[\r\n\t]+/g, " ")
    .replace(/[<>]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 32)
}

function getFirstNameOnly(value) {
  const sanitized = sanitizeDonorDisplayName(value)
    .replace(/^streamtip\s*\/\s*/i, "")
    .replace(/^from\s+/i, "")
    .trim()

  if (!sanitized) {
    return "Anonymous"
  }

  return sanitized.split(/\s+/)[0] || "Anonymous"
}

function isSystemNarration(value, creatorNames = []) {
  const sanitized = sanitizeDonorDisplayName(value)
  const normalized = normalizeName(sanitized)

  if (!normalized) return true
  if (creatorNames.includes(normalized)) return true
  if (normalized.startsWith("streamtip ")) return true
  if (/^(transfer|bank transfer|payment|donation|gift|monnify|moniepoint|wallet funding)$/i.test(sanitized)) {
    return true
  }
  if (/^(mfy|mnfy|stp|stip|trf|txn|ref)[\s/-]*[a-z0-9-]{5,}$/i.test(sanitized)) {
    return true
  }
  if (/^[a-z0-9-]{18,}$/i.test(sanitized) && /\d/.test(sanitized)) {
    return true
  }
  if (/^\d{6,}$/.test(sanitized)) {
    return true
  }

  return false
}

function getDonationSenderName(eventData, data, creator) {
  const creatorNames = [
    creator?.name,
    creator?.virtualAccount?.accountName,
    `StreamTip/${creator?.name || ""}`,
    eventData?.customer?.name,
    eventData?.customerName,
  ]
    .map(normalizeName)
    .filter(Boolean)

  const narrationPaths = [
    "narration",
    "remark",
    "remarks",
    "reference",
    "senderReference",
    "customerReference",
    "paymentDescription",
    "paymentNarration",
    "transactionNarration",
    "transactionDescription",
    "transactionRemark",
    "description",
    "note",
    "meta.narration",
    "meta.remark",
    "metadata.narration",
    "metadata.remark",
  ]

  const narrationCandidates = [
    ...narrationPaths.map((path) => getNestedValue(eventData, path)),
    ...narrationPaths.map((path) => getNestedValue(data, path)),
  ]

  for (const candidate of narrationCandidates) {
    const nickname = sanitizeDonorDisplayName(candidate)

    if (!nickname || isSystemNarration(nickname, creatorNames)) {
      continue
    }

    return {
      name: nickname,
      source: "narration",
    }
  }

  const senderNamePaths = [
    "paymentSourceInformation.accountName",
    "paymentSourceInformation.accountHolderName",
    "paymentSourceInformation.originatorAccountName",
    "sourceAccountInformation.accountName",
    "sourceAccountInformation.accountHolderName",
    "originatorAccountName",
    "sourceAccountName",
    "accountName",
    "payerName",
    "payer.accountName",
    "payer.name",
  ]

  const candidates = [
    ...senderNamePaths.map((path) => getNestedValue(eventData, path)),
    ...senderNamePaths.map((path) => getNestedValue(data, path)),
    data?.sender,
    data?.payerName,
  ]

  for (const candidate of candidates) {
    const sender = String(candidate || "").trim()
    const normalizedSender = normalizeName(sender)

    if (!sender || !normalizedSender || creatorNames.includes(normalizedSender)) {
      continue
    }

    if (normalizedSender.startsWith("streamtip ")) {
      continue
    }

    return {
      name: getFirstNameOnly(sender),
      source: "account_first_name",
    }
  }

  return {
    name: "Anonymous",
    source: "anonymous",
  }
}

function sanitizeUser(user) {
  if (!user) return null

  return {
    id: user._id,
    name: user.name,
    email: user.email,
    overlaySlug: getOverlaySlug(user),
    role: user.role || "creator",
    status: user.status || "active",
    profileImage: user.profileImage || "",
    identity: sanitizeIdentity(user.identity),
    virtualAccount: user.virtualAccount || null,
    createdAt: user.createdAt,
  }
}

function sanitizePublicOverlayUser(user) {
  const sanitized = sanitizeUser(user)

  if (!sanitized) {
    return null
  }

  return {
    ...sanitized,
    email: "",
    identity: undefined,
  }
}

function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString("hex")
  const hash = crypto.scryptSync(password, salt, 64).toString("hex")
  return `${salt}:${hash}`
}

function verifyPassword(password, passwordHash) {
  if (!passwordHash || !passwordHash.includes(":")) {
    return false
  }

  const [salt, storedHash] = passwordHash.split(":")
  const computedHash = crypto.scryptSync(password, salt, 64).toString("hex")
  return crypto.timingSafeEqual(
    Buffer.from(storedHash, "hex"),
    Buffer.from(computedHash, "hex"),
  )
}

function generateSessionToken() {
  return crypto.randomBytes(32).toString("hex")
}

function safeTimingEqual(left, right) {
  const leftBuffer = Buffer.from(String(left || ""), "utf8")
  const rightBuffer = Buffer.from(String(right || ""), "utf8")

  if (leftBuffer.length !== rightBuffer.length) {
    return false
  }

  return crypto.timingSafeEqual(leftBuffer, rightBuffer)
}

function verifyMonnifySignature(req) {
  if (!MONNIFY_SECRET_KEY) {
    return false
  }

  const headerSignature = String(req.headers["monnify-signature"] || "").trim()
  const rawBody = typeof req.rawBody === "string" ? req.rawBody : JSON.stringify(req.body || {})

  if (!headerSignature || !rawBody) {
    return false
  }

  const expectedSignature = crypto
    .createHmac("sha512", MONNIFY_SECRET_KEY)
    .update(rawBody, "utf8")
    .digest("hex")

  return safeTimingEqual(headerSignature, expectedSignature)
}

function parseMoneyAmount(value) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value
  }

  const normalized = String(value || "")
    .replace(/,/g, "")
    .replace(/[^\d.-]/g, "")
    .trim()
  const parsed = Number(normalized)

  return Number.isFinite(parsed) ? parsed : 0
}

function firstValidMoneyAmount(...values) {
  for (const value of values) {
    const amount = parseMoneyAmount(value)

    if (amount > 0) {
      return amount
    }
  }

  return 0
}

function calculateRevenueSplit(amount) {
  const gross = parseMoneyAmount(amount)
  const platformFee = Math.round(gross * PLATFORM_FEE_RATE)
  const creatorShare = Math.max(0, gross - platformFee)

  return {
    gross,
    platformFee,
    creatorShare,
  }
}

async function createAuditLog({ actorType, actorId = "", eventType, message, metadata = {} }) {
  try {
    await AuditLog.create({
      actorType,
      actorId,
      eventType,
      message,
      metadata,
      createdAt: new Date(),
    })
  } catch (error) {
    console.error("Failed to create audit log", error)
  }
}

function getAxiosErrorMessage(error, fallbackMessage) {
  if (!(error instanceof AxiosError)) {
    return fallbackMessage
  }

  const monnifyMessage =
    error.response?.data?.responseMessage ||
    error.response?.data?.responseBody?.message ||
    error.response?.data?.message

  if (typeof monnifyMessage === "string" && monnifyMessage.trim()) {
    return monnifyMessage.trim()
  }

  return error.message || fallbackMessage
}

function buildMonnifyCustomerEmail(email, suffix) {
  const normalizedEmail = String(email || "").toLowerCase().trim()
  const [localPart, domain = "streamtip.local"] = normalizedEmail.split("@")
  const safeLocalPart = (localPart || "creator").replace(/[^a-z0-9._+-]/gi, "")

  return `${safeLocalPart}+${suffix}@${domain}`
}

function buildMonnifyCustomerName(name, suffix) {
  const trimmedName = String(name || "StreamTip Creator").trim() || "StreamTip Creator"
  return `${trimmedName} ${suffix}`.trim()
}

function isDuplicateReservedAccountError(error) {
  const message = getAxiosErrorMessage(error, "").toLowerCase()
  return message.includes("cannot reserve more than 1 account")
}

function isTemporaryMonnifyError(error) {
  if (error instanceof AxiosError) {
    const status = Number(error.response?.status || 0)
    if (status >= 500) {
      return true
    }
  }

  const message = getAxiosErrorMessage(error, "").toLowerCase()
  return (
    message.includes("service is currently unavailable") ||
    message.includes("try again later") ||
    message.includes("timeout") ||
    message.includes("temporarily unavailable")
  )
}

function wait(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms)
  })
}

async function markVirtualAccountPending(user, reason = "") {
  user.virtualAccount = {
    accountReference: `PENDING-${user._id}`,
    accountName: `StreamTip/${user.name}`,
    accountNumber: "Pending provisioning",
    bankName: "Monnify",
    bankCode: "",
    reservationReference: "",
    status: "pending",
    provider: "monnify",
    environment: getMonnifyEnvironment(),
    createdAt: new Date(),
  }

  await user.save()

  await createAuditLog({
    actorType: "system",
    actorId: user._id.toString(),
    eventType: "monnify.virtual_account.pending",
    message: `Virtual account moved to pending for ${user.email}.`,
    metadata: { reason },
  })
}

function decodeJwtPart(value) {
  const normalized = value.replace(/-/g, "+").replace(/_/g, "/")
  const padded = normalized + "=".repeat((4 - (normalized.length % 4 || 4)) % 4)
  return Buffer.from(padded, "base64").toString("utf8")
}

function parseJwt(token) {
  const parts = String(token || "").split(".")
  if (parts.length !== 3) {
    throw new Error("Invalid token format.")
  }

  return {
    encodedHeader: parts[0],
    encodedPayload: parts[1],
    signature: parts[2],
    header: JSON.parse(decodeJwtPart(parts[0])),
    payload: JSON.parse(decodeJwtPart(parts[1])),
    signingInput: `${parts[0]}.${parts[1]}`,
  }
}

async function verifyGoogleIdToken(idToken) {
  if (!GOOGLE_CLIENT_ID) {
    throw new Error("Google sign-in is not configured yet.")
  }

  const response = await axios.get("https://oauth2.googleapis.com/tokeninfo", {
    params: { id_token: idToken },
  })

  const payload = response.data || {}
  const audiences = String(payload.aud || "").split(",").map((item) => item.trim())

  if (!audiences.includes(GOOGLE_CLIENT_ID)) {
    throw new Error("Google token audience mismatch.")
  }

  if (!payload.email) {
    throw new Error("Google did not return an email address.")
  }

  return {
    sub: String(payload.sub || ""),
    email: String(payload.email).toLowerCase().trim(),
    name: String(payload.name || payload.given_name || "Google Creator").trim(),
    picture: String(payload.picture || "").trim(),
  }
}

async function verifyAppleIdentityToken(identityToken) {
  if (!APPLE_CLIENT_ID) {
    throw new Error("Apple sign-in is not configured yet.")
  }

  const parsed = parseJwt(identityToken)
  const response = await axios.get("https://appleid.apple.com/auth/keys")
  const keys = Array.isArray(response.data?.keys) ? response.data.keys : []
  const matchingKey = keys.find(
    (key) => key.kid === parsed.header.kid && key.alg === parsed.header.alg,
  )

  if (!matchingKey) {
    throw new Error("Unable to find matching Apple signing key.")
  }

  const publicKey = crypto.createPublicKey({
    key: matchingKey,
    format: "jwk",
  })
  const signature = Buffer.from(
    parsed.signature.replace(/-/g, "+").replace(/_/g, "/"),
    "base64",
  )

  const verified = crypto.verify(
    "RSA-SHA256",
    Buffer.from(parsed.signingInput),
    publicKey,
    signature,
  )

  if (!verified) {
    throw new Error("Apple identity token verification failed.")
  }

  const payload = parsed.payload || {}

  if (payload.iss !== "https://appleid.apple.com") {
    throw new Error("Apple token issuer mismatch.")
  }

  const audience = Array.isArray(payload.aud) ? payload.aud : [payload.aud]
  if (!audience.includes(APPLE_CLIENT_ID)) {
    throw new Error("Apple token audience mismatch.")
  }

  if (!payload.email) {
    throw new Error("Apple did not return an email address.")
  }

  return {
    sub: String(payload.sub || ""),
    email: String(payload.email).toLowerCase().trim(),
    name: String(payload.email || "Apple Creator").split("@")[0],
    picture: "",
  }
}

async function createOrLoginOAuthUser({ provider, sub, email, name, picture = "" }) {
  const providerField = provider === "google" ? "googleSub" : "appleSub"
  let user =
    (await User.findOne({ [providerField]: sub })) ||
    (await User.findOne({ email }))

  if (!user) {
    user = await User.create({
      name,
      email,
      passwordHash: hashPassword(generateSessionToken()),
      sessionToken: generateSessionToken(),
      role: "creator",
      status: "active",
      profileImage: picture,
      [providerField]: sub,
    })

    await createReservedAccountForUser(user)

    await createAuditLog({
      actorType: "user",
      actorId: user._id.toString(),
      eventType: `user.oauth_registered.${provider}`,
      message: `${email} registered with ${provider}.`,
      metadata: { email, provider },
    })
  } else {
    if (!user[providerField]) {
      user[providerField] = sub
    }

    if (!user.profileImage && picture) {
      user.profileImage = picture
    }

    if (!user.virtualAccount?.accountNumber) {
      await createReservedAccountForUser(user)
    }
  }

  if (user.status === "banned") {
    throw new Error("This account has been banned.")
  }

  if (user.status === "suspended") {
    throw new Error("This account is currently suspended.")
  }

  user.sessionToken = generateSessionToken()
  await user.save()

  await createAuditLog({
    actorType: "user",
    actorId: user._id.toString(),
    eventType: `user.oauth_login.${provider}`,
    message: `${user.email} signed in with ${provider}.`,
    metadata: { email: user.email, provider },
  })

  return user
}

async function getMonnifyAccessToken() {
  const credentials = Buffer.from(`${MONNIFY_API_KEY}:${MONNIFY_SECRET_KEY}`).toString("base64")

  const response = await axios.post(
    `${MONNIFY_BASE_URL}/api/v1/auth/login`,
    {},
    {
      headers: {
        Authorization: `Basic ${credentials}`,
      },
    },
  )

  return response.data?.responseBody?.accessToken
}

async function getSupportedBanks() {
  if (!isMonnifyConfigured()) {
    return fallbackBanks
  }

  try {
    const accessToken = await getMonnifyAccessToken()
    const response = await axios.get(`${MONNIFY_BASE_URL}/api/v1/banks`, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        Accept: "application/json",
      },
    })

    const responseBody = Array.isArray(response.data?.responseBody) ? response.data.responseBody : []
    const banks = responseBody
      .map((bank) => ({
        name: String(bank?.name || "").trim(),
        code: String(bank?.code || "").trim(),
      }))
      .filter((bank) => bank.name && bank.code)

    return banks.length ? banks : fallbackBanks
  } catch (error) {
    console.error("Failed to load Monnify banks, using fallback bank list.", error)
    return fallbackBanks
  }
}

async function resolveBankAccountName({ bankCode, accountNumber }) {
  if (!isMonnifyConfigured()) {
    throw new Error("Monnify account validation is not configured yet.")
  }

  const accessToken = await getMonnifyAccessToken()
  const response = await axios.get(`${MONNIFY_BASE_URL}/api/v1/disbursements/account/validate`, {
    headers: {
      Authorization: `Bearer ${accessToken}`,
      Accept: "application/json",
    },
    params: {
      accountNumber,
      bankCode,
    },
  })

  const responseBody = response.data?.responseBody || {}
  const accountName = String(responseBody.accountName || "").trim()

  if (!accountName) {
    throw new Error("Monnify did not return an account name for that bank account.")
  }

  return {
    accountName,
    accountNumber: String(responseBody.accountNumber || accountNumber).trim(),
    bankCode: String(responseBody.bankCode || bankCode).trim(),
  }
}

async function createReservedAccountForUser(user) {
  if (!isMonnifyConfigured()) {
    throw new Error(
      "Monnify is not configured yet. Add MONNIFY_API_KEY, MONNIFY_SECRET_KEY, MONNIFY_CONTRACT_CODE, and MONNIFY_BASE_URL to Backend/.env.",
    )
  }

  const currentEnvironment = getMonnifyEnvironment()

  if (
    user.virtualAccount?.accountNumber &&
    user.virtualAccount.status === "active" &&
    user.virtualAccount.environment === currentEnvironment
  ) {
    return user.virtualAccount
  }

  const bvn = String(user.identity?.bvn || "").trim()
  const nin = String(user.identity?.nin || "").trim()

  if (requiresMonnifyCustomerVerification() && !bvn && !nin) {
    throw new Error(
      "Live Monnify setup requires the creator's BVN or NIN before a virtual account can be provisioned.",
    )
  }

  const accessToken = await getMonnifyAccessToken()
  const accountReference = `STIP-${user._id}-${Date.now()}`
  const monnifySuffix = `streamtip-${String(user._id).slice(-6)}`
  const monnifyCustomerEmail = buildMonnifyCustomerEmail(user.email, monnifySuffix)
  const monnifyCustomerName = buildMonnifyCustomerName(user.name, monnifySuffix)

  const requestReservedAccount = async (customerEmail) =>
    axios.post(
      `${MONNIFY_BASE_URL}/api/v2/bank-transfer/reserved-accounts`,
      {
        accountReference,
        accountName: `StreamTip/${user.name}`,
        currencyCode: "NGN",
        contractCode: MONNIFY_CONTRACT_CODE,
        customerEmail,
        customerName: monnifyCustomerName,
        getAllAvailableBanks: true,
        ...(bvn ? { bvn } : {}),
        ...(nin ? { nin } : {}),
      },
      {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          "Content-Type": "application/json",
        },
      },
    )

  let response

  try {
    response = await requestReservedAccount(monnifyCustomerEmail)
  } catch (error) {
    if (isDuplicateReservedAccountError(error)) {
      const fallbackCustomerEmail = buildMonnifyCustomerEmail(
        user.email,
        `${monnifySuffix}-${Date.now()}`,
      )

      try {
        response = await requestReservedAccount(fallbackCustomerEmail)
      } catch (retryError) {
        throw new Error(
          getAxiosErrorMessage(retryError, "Could not create a Monnify reserved account."),
        )
      }
    } else if (isTemporaryMonnifyError(error)) {
      let lastError = error

      for (const retryDelay of [700, 1500]) {
        await wait(retryDelay)

        try {
          response = await requestReservedAccount(monnifyCustomerEmail)
          lastError = null
          break
        } catch (retryError) {
          lastError = retryError
        }
      }

      if (!response && lastError) {
        throw new Error(
          getAxiosErrorMessage(lastError, "Could not create a Monnify reserved account."),
        )
      }
    } else {
      throw new Error(
        getAxiosErrorMessage(error, "Could not create a Monnify reserved account."),
      )
    }
  }

  const responseBody = response.data?.responseBody || {}
  const account =
    Array.isArray(responseBody.accounts) && responseBody.accounts.length > 0
      ? responseBody.accounts[0]
      : responseBody

  if (!account?.accountNumber) {
    throw new Error("Monnify did not return a reserved account.")
  }

  const virtualAccount = {
    accountReference,
    accountName: account.accountName || responseBody.accountName || `StreamTip/${user.name}`,
    accountNumber: account.accountNumber,
    bankName: account.bankName || "Monnify",
    bankCode: account.bankCode || "",
    reservationReference: responseBody.reservationReference || "",
    status: "active",
    provider: "monnify",
    environment: currentEnvironment,
    createdAt: new Date(),
  }

  user.virtualAccount = virtualAccount
  await user.save()

  return virtualAccount
}

function createTransferReference(prefix = "STIP-PAYOUT") {
  return `${prefix}-${Date.now()}-${Math.round(Math.random() * 1_000_000)}`
}

function normalizeMonnifyTransferStatus(status) {
  const normalized = String(status || "").toUpperCase()

  if (["SUCCESS", "SUCCESSFUL", "COMPLETED"].includes(normalized)) {
    return "completed"
  }

  if (["FAILED", "REJECTED", "CANCELLED", "EXPIRED"].includes(normalized)) {
    return "failed"
  }

  return "pending"
}

async function initiateMonnifyDisbursement({
  amount,
  bankCode,
  accountNumber,
  accountName,
  narration,
  reference,
}) {
  if (!isMonnifyConfigured()) {
    throw new Error("Monnify disbursement is not configured yet.")
  }

  if (!MONNIFY_DISBURSEMENT_SOURCE_ACCOUNT_NUMBER) {
    throw new Error(
      "MONNIFY_DISBURSEMENT_SOURCE_ACCOUNT_NUMBER is missing from Backend/.env.",
    )
  }

  const accessToken = await getMonnifyAccessToken()
  const response = await axios.post(
    `${MONNIFY_BASE_URL}/api/v2/disbursements/single`,
    {
      amount,
      reference,
      narration,
      destinationBankCode: bankCode,
      destinationAccountNumber: accountNumber,
      destinationAccountName: accountName,
      currency: "NGN",
      sourceAccountNumber: MONNIFY_DISBURSEMENT_SOURCE_ACCOUNT_NUMBER,
    },
    {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": "application/json",
      },
    },
  )

  return response.data?.responseBody || {}
}

async function getCurrentUser() {
  return User.findOne().sort({ createdAt: -1 })
}

async function getRevenueTotals({ creatorId } = {}) {
  const donationQuery = creatorId ? { creatorId } : {}
  const payoutQuery = {
    status: { $ne: "failed" },
    ...(creatorId ? { creatorId } : {}),
  }
  const platformWithdrawalQuery = creatorId ? { _id: null } : { status: { $ne: "failed" } }

  const [donations, payouts, platformWithdrawals] = await Promise.all([
    Donation.find(donationQuery),
    Payout.find(payoutQuery),
    creatorId ? [] : PlatformWithdrawal.find(platformWithdrawalQuery),
  ])

  const grossRevenue = donations.reduce((sum, donation) => sum + (Number(donation.amount) || 0), 0)
  const platformRevenue = donations.reduce((sum, donation) => {
    if (typeof donation.platformFee === "number") {
      return sum + donation.platformFee
    }

    return sum + calculateRevenueSplit(donation.amount).platformFee
  }, 0)
  const creatorRevenue = donations.reduce((sum, donation) => {
    if (typeof donation.creatorShare === "number") {
      return sum + donation.creatorShare
    }

    return sum + calculateRevenueSplit(donation.amount).creatorShare
  }, 0)
  const totalPaidOut = payouts.reduce((sum, payout) => sum + (Number(payout.amount) || 0), 0)
  const totalPlatformWithdrawn = platformWithdrawals.reduce(
    (sum, withdrawal) => sum + (Number(withdrawal.amount) || 0),
    0,
  )

  return {
    grossRevenue,
    platformRevenue,
    creatorRevenue,
    totalPaidOut,
    totalPlatformWithdrawn,
    creatorAvailableBalance: Math.max(0, creatorRevenue - totalPaidOut),
    pendingPlatformRevenue: Math.max(0, platformRevenue - totalPlatformWithdrawn),
  }
}

function groupTransactionsByPeriod(withdrawals) {
  const yearMap = new Map()

  withdrawals.forEach((withdrawal) => {
    const createdAt = withdrawal.createdAt ? new Date(withdrawal.createdAt) : new Date()
    const year = String(createdAt.getFullYear())
    const monthKey = new Intl.DateTimeFormat("en-NG", {
      month: "long",
      year: "numeric",
    }).format(createdAt)

    if (!yearMap.has(year)) {
      yearMap.set(year, new Map())
    }

    const monthMap = yearMap.get(year)
    if (!monthMap.has(monthKey)) {
      monthMap.set(monthKey, [])
    }

    monthMap.get(monthKey).push(withdrawal)
  })

  return Array.from(yearMap.entries())
    .sort((a, b) => Number(b[0]) - Number(a[0]))
    .map(([year, monthMap]) => ({
      year,
      months: Array.from(monthMap.entries()).map(([month, items]) => ({
        month,
        items,
      })),
    }))
}

async function getSessionUser(req) {
  const sessionToken = String(req.headers["x-session-token"] || "").trim()

  if (!sessionToken) {
    return null
  }

  return User.findOne({ sessionToken })
}

async function getSessionUserByToken(sessionToken) {
  const normalizedToken = String(sessionToken || "").trim()

  if (!normalizedToken) {
    return null
  }

  return User.findOne({ sessionToken: normalizedToken })
}

async function requireSessionUser(req, res, next) {
  try {
    const user = await getSessionUser(req)

    if (!user) {
      return res.status(401).json({ error: "Authentication required." })
    }

    if (user.status && user.status !== "active") {
      return res.status(403).json({ error: "This account is not active anymore." })
    }

    req.user = user
    return next()
  } catch (error) {
    console.error(error)
    return res.status(500).json({ error: "Failed to validate user session." })
  }
}

async function findCreatorForMonnifyEvent(eventData) {
  const accountReference = String(eventData?.product?.reference || "").trim()
  const destinationAccountNumber = String(
    eventData?.destinationAccountInformation?.accountNumber || "",
  ).trim()
  const destinationBankCode = String(
    eventData?.destinationAccountInformation?.bankCode || "",
  ).trim()

  if (accountReference) {
    const matchedByReference = await User.findOne({
      $or: [
        { "virtualAccount.accountReference": accountReference },
        { "virtualAccount.reservationReference": accountReference },
      ],
    })

    if (matchedByReference) {
      return matchedByReference
    }
  }

  if (destinationAccountNumber) {
    const candidates = await User.find({
      "virtualAccount.accountNumber": destinationAccountNumber,
      ...(destinationBankCode ? { "virtualAccount.bankCode": destinationBankCode } : {}),
    }).limit(2)

    if (candidates.length === 1) {
      return candidates[0]
    }
  }

  return null
}

async function requireAdminSession(req, res, next) {
  try {
    const adminToken = String(req.headers["x-admin-token"] || "").trim()

    if (!adminToken) {
      return res.status(401).json({ error: "Admin authentication required." })
    }

    const adminSession = await AdminSession.findOne({ token: adminToken })

    if (!adminSession) {
      return res.status(401).json({ error: "Invalid admin session." })
    }

    adminSession.lastSeenAt = new Date()
    await adminSession.save()

    req.adminSession = adminSession
    return next()
  } catch (error) {
    console.error(error)
    return res.status(500).json({ error: "Failed to validate admin session." })
  }
}

io.use(async (socket, next) => {
  try {
    const sessionToken = String(
      socket.handshake.auth?.sessionToken || socket.handshake.headers["x-session-token"] || "",
    ).trim()
    const overlayCreatorId = String(socket.handshake.auth?.overlayCreatorId || "").trim()

    if (overlayCreatorId && mongoose.Types.ObjectId.isValid(overlayCreatorId)) {
      const creator = await User.findById(overlayCreatorId)

      if (creator && (!creator.status || creator.status === "active")) {
        socket.data.user = null
        socket.data.overlayCreatorId = creator._id
        socket.join(getCreatorRoom(creator._id))
      }

      return next()
    }

    if (!sessionToken) {
      socket.data.user = null
      return next()
    }

    const user = await getSessionUserByToken(sessionToken)

    if (!user || (user.status && user.status !== "active")) {
      socket.data.user = null
      return next()
    }

    socket.data.user = user
    socket.join(getCreatorRoom(user._id))
    return next()
  } catch (error) {
    return next(error)
  }
})

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "streamtip-api",
    database: isDatabaseConnected() ? "connected" : "disconnected",
    databaseReadyState: mongoose.connection.readyState,
    mongodbUriConfigured: Boolean(MONGODB_URI),
    mongodbHost: getMongoUriHost(),
    lastDatabaseError,
    environment: getMonnifyEnvironment(),
    uptime: Math.round(process.uptime()),
  })
})

app.use(requireDatabaseReady)

app.post("/auth/register", async (req, res) => {
  try {
    const { name, email, password, bvn = "", nin = "" } = req.body

    if (!name || !email || !password) {
      return res.status(400).json({ error: "Name, email, and password are required." })
    }

    const normalizedEmail = String(email).toLowerCase().trim()
    const trimmedName = String(name).trim()
    const rawPassword = String(password)
    const trimmedBvn = String(bvn || "").trim()
    const trimmedNin = String(nin || "").trim()

    if (rawPassword.length < 8) {
      return res.status(400).json({ error: "Password must be at least 8 characters long." })
    }

    if (trimmedBvn && !/^\d{11}$/.test(trimmedBvn)) {
      return res.status(400).json({ error: "BVN must be 11 digits." })
    }

    if (trimmedNin && !/^\d{11}$/.test(trimmedNin)) {
      return res.status(400).json({ error: "NIN must be 11 digits." })
    }

    let user = await User.findOne({ email: normalizedEmail })

    if (user) {
      return res.status(409).json({ error: "An account with that email already exists." })
    }

    user = await User.create({
      name: trimmedName,
      email: normalizedEmail,
      passwordHash: hashPassword(rawPassword),
      sessionToken: generateSessionToken(),
      role: "creator",
      status: "active",
      identity: {
        bvn: trimmedBvn,
        nin: trimmedNin,
      },
    })

    await createAuditLog({
      actorType: "user",
      actorId: user._id.toString(),
      eventType: "user.registered",
      message: `${normalizedEmail} registered.`,
      metadata: {
        name: trimmedName,
        email: normalizedEmail,
      },
    })

    let warning = ""

    try {
      await createReservedAccountForUser(user)
    } catch (error) {
      const reason =
        error instanceof Error ? error.message : "Could not provision a Monnify reserved account."

      await markVirtualAccountPending(user, reason)
      warning = isTemporaryMonnifyError(error)
        ? "Your account was created, but Monnify is temporarily unavailable. Your virtual account is pending provisioning and can be retried later."
        : "Your account was created, but the virtual account could not be provisioned yet. It has been marked as pending so you can continue into the dashboard."
    }

    return res.json({
      user: sanitizeUser(user),
      sessionToken: user.sessionToken,
      warning,
    })
  } catch (error) {
    console.error(error)
    return res.status(500).json({ error: "Failed to register user." })
  }
})

app.post("/auth/login", async (req, res) => {
  try {
    const { email, password } = req.body

    if (!email || !password) {
      return res.status(400).json({ error: "Email and password are required." })
    }

    const user = await User.findOne({ email: String(email).toLowerCase().trim() })

    if (!user || !verifyPassword(String(password), user.passwordHash)) {
      return res.status(401).json({ error: "Invalid email or password." })
    }

    if (user.status === "banned") {
      return res.status(403).json({ error: "This account has been banned." })
    }

    if (user.status === "suspended") {
      return res.status(403).json({ error: "This account is currently suspended." })
    }

    user.sessionToken = generateSessionToken()
    await user.save()

    await createAuditLog({
      actorType: "user",
      actorId: user._id.toString(),
      eventType: "user.login",
      message: `${user.email} logged in.`,
      metadata: { email: user.email },
    })

    return res.json({
      user: sanitizeUser(user),
      sessionToken: user.sessionToken,
    })
  } catch (error) {
    console.error(error)
    return res.status(500).json({ error: "Failed to log in." })
  }
})

app.post("/auth/oauth/google", async (req, res) => {
  try {
    const credential = String(req.body?.credential || "").trim()

    if (!credential) {
      return res.status(400).json({ error: "Google credential is required." })
    }

    const identity = await verifyGoogleIdToken(credential)
    const user = await createOrLoginOAuthUser({
      provider: "google",
      sub: identity.sub,
      email: identity.email,
      name: identity.name,
      picture: identity.picture,
    })

    return res.json({
      user: sanitizeUser(user),
      sessionToken: user.sessionToken,
    })
  } catch (error) {
    console.error(error)
    return res.status(400).json({
      error: error instanceof Error ? error.message : "Google sign-in failed.",
    })
  }
})

app.post("/auth/oauth/apple", async (req, res) => {
  try {
    const identityToken = String(req.body?.identityToken || "").trim()

    if (!identityToken) {
      return res.status(400).json({ error: "Apple identity token is required." })
    }

    const identity = await verifyAppleIdentityToken(identityToken)
    const user = await createOrLoginOAuthUser({
      provider: "apple",
      sub: identity.sub,
      email: identity.email,
      name: identity.name,
      picture: identity.picture,
    })

    return res.json({
      user: sanitizeUser(user),
      sessionToken: user.sessionToken,
    })
  } catch (error) {
    console.error(error)
    return res.status(400).json({
      error: error instanceof Error ? error.message : "Apple sign-in failed.",
    })
  }
})

app.get("/auth/me", async (req, res) => {
  try {
    const user = await getSessionUser(req)

    if (!user) {
      return res.status(401).json({ error: "Not authenticated." })
    }

    if (user.status && user.status !== "active") {
      user.sessionToken = null
      await user.save()
      return res.status(403).json({ error: "This account is not active anymore." })
    }

    return res.json({ user: sanitizeUser(user) })
  } catch (error) {
    console.error(error)
    return res.status(500).json({ error: "Failed to load current user." })
  }
})

app.post("/auth/logout", async (req, res) => {
  try {
    const user = await getSessionUser(req)

    if (user) {
      user.sessionToken = null
      await user.save()
      await createAuditLog({
        actorType: "user",
        actorId: user._id.toString(),
        eventType: "user.logout",
        message: `${user.email} logged out.`,
        metadata: { email: user.email },
      })
    }

    return res.json({ success: true })
  } catch (error) {
    console.error(error)
    return res.status(500).json({ error: "Failed to log out." })
  }
})

app.post("/admin/auth/login", async (req, res) => {
  try {
    const { email, password } = req.body

    if (!email || !password) {
      return res.status(400).json({ error: "Email and password are required." })
    }

    const normalizedEmail = String(email).toLowerCase().trim()

    if (!ADMIN_EMAIL || !ADMIN_PASSWORD) {
      return res.status(503).json({
        error: "Admin login is not configured. Set ADMIN_EMAIL and ADMIN_PASSWORD on the backend.",
      })
    }

    if (
      normalizedEmail !== String(ADMIN_EMAIL).toLowerCase().trim() ||
      String(password) !== String(ADMIN_PASSWORD)
    ) {
      await createAuditLog({
        actorType: "admin",
        eventType: "admin.login.failed",
        message: "Failed admin login attempt.",
        metadata: { email: normalizedEmail },
      })
      return res.status(401).json({ error: "Invalid admin credentials." })
    }

    const token = generateSessionToken()
    const session = await AdminSession.create({
      token,
      createdAt: new Date(),
      lastSeenAt: new Date(),
    })

    await createAuditLog({
      actorType: "admin",
      actorId: session._id.toString(),
      eventType: "admin.login.success",
      message: "Admin logged in.",
      metadata: { email: normalizedEmail },
    })

    return res.json({
      token,
      admin: {
        email: normalizedEmail,
      },
    })
  } catch (error) {
    console.error(error)
    return res.status(500).json({ error: "Failed to log in as admin." })
  }
})

app.get("/admin/auth/me", requireAdminSession, async (req, res) => {
  return res.json({
    admin: {
      email: String(ADMIN_EMAIL).toLowerCase().trim(),
    },
  })
})

app.post("/admin/auth/logout", requireAdminSession, async (req, res) => {
  try {
    await AdminSession.deleteOne({ _id: req.adminSession._id })
    await createAuditLog({
      actorType: "admin",
      actorId: req.adminSession._id.toString(),
      eventType: "admin.logout",
      message: "Admin logged out.",
    })
    return res.json({ success: true })
  } catch (error) {
    console.error(error)
    return res.status(500).json({ error: "Failed to log out admin." })
  }
})

app.post("/users/:id/virtual-account", requireSessionUser, async (req, res) => {
  try {
    if (String(req.user._id) !== String(req.params.id)) {
      return res.status(403).json({ error: "You can only provision a virtual account for your own user." })
    }

    const user = await User.findById(req.params.id)

    if (!user) {
      return res.status(404).json({ error: "User not found." })
    }

    const virtualAccount = await createReservedAccountForUser(user)
    return res.json({ virtualAccount })
  } catch (error) {
    console.error(error)
    return res.status(502).json({
      error:
        error instanceof Error
          ? error.message
          : "Could not provision a Monnify reserved account.",
    })
  }
})

app.post("/webhook/monnify", async (req, res) => {
  try {
    if (!isWebhookIpAllowed(req)) {
      await createAuditLog({
        actorType: "system",
        eventType: "webhook.monnify.rejected_ip",
        message: "Rejected Monnify webhook from a non-allowlisted IP.",
        metadata: {
          ipAddresses: getRequestIpAddresses(req),
        },
      })

      return res.status(403).json({ error: "Webhook source IP is not allowed." })
    }

    if (!verifyMonnifySignature(req)) {
      await createAuditLog({
        actorType: "system",
        eventType: "webhook.monnify.invalid_signature",
        message: "Rejected Monnify webhook with invalid signature.",
        metadata: {
          ip: req.ip,
        },
      })

      return res.status(401).json({ error: "Invalid Monnify signature." })
    }

    const data = req.body || {}
    const eventData =
      (data.eventData && typeof data.eventData === "object" ? data.eventData : data.responseBody) ||
      data
    const creator = await findCreatorForMonnifyEvent(eventData)
    const eventType = String(data.eventType || eventData.eventType || "monnify.webhook")
    const paymentStatus = String(
      eventData.paymentStatus || eventData.status || data.paymentStatus || "PENDING",
    ).toUpperCase()
    const transactionReference = String(
      eventData.transactionReference || eventData.transactionRef || "",
    ).trim()
    const paymentReference = String(
      eventData.paymentReference || eventData.transactionHash || "",
    ).trim()

    if (paymentStatus && paymentStatus !== "PAID") {
      await createAuditLog({
        actorType: "system",
        eventType: "webhook.monnify.ignored",
        message: `Ignored Monnify webhook with payment status ${paymentStatus}.`,
        metadata: {
          eventType,
          paymentStatus,
          transactionReference,
          paymentReference,
        },
      })

      return res.sendStatus(200)
    }

    if (transactionReference) {
      const existingByTransactionRef = await Donation.findOne({
        monnifyTransactionReference: transactionReference,
      })

      if (existingByTransactionRef) {
        return res.sendStatus(200)
      }
    }

    if (paymentReference) {
      const existingByPaymentRef = await Donation.findOne({
        monnifyPaymentReference: paymentReference,
      })

      if (existingByPaymentRef) {
        return res.sendStatus(200)
      }
    }

    const destinationAccountNumber = String(
      eventData.destinationAccountInformation?.accountNumber || "",
    ).trim()
    const destinationBankName = String(
      eventData.destinationAccountInformation?.bankName || "",
    ).trim()
    const grossAmount = firstValidMoneyAmount(
      eventData.amountPaid,
      eventData.amount,
      eventData.settlementAmount,
      eventData.totalPayable,
      data.amount,
      data.amountPaid,
    )
    const split = calculateRevenueSplit(grossAmount)

    if (split.gross <= 0) {
      await createAuditLog({
        actorType: "system",
        eventType: "webhook.monnify.invalid_amount",
        message: "Monnify webhook had a paid status but no valid donation amount.",
        metadata: {
          eventType,
          transactionReference,
          paymentReference,
          amountPaid: eventData.amountPaid,
          amount: eventData.amount || data.amount,
          dataAmountPaid: data.amountPaid,
        },
      })

      return res.sendStatus(200)
    }

    if (!creator) {
      await createAuditLog({
        actorType: "system",
        eventType: "webhook.monnify.unmatched_creator",
        message: "Monnify webhook could not be matched to a registered creator.",
        metadata: {
          eventType,
          destinationAccountNumber,
          transactionReference,
          paymentReference,
          accountReference: String(eventData?.product?.reference || ""),
        },
      })

      return res.sendStatus(200)
    }

    const senderDisplay = getDonationSenderName(eventData, data, creator)
    const donation = await Donation.create({
      creatorId: creator._id,
      creatorEmail: creator.email,
      sender: senderDisplay.name,
      senderNameSource: senderDisplay.source,
      amount: split.gross,
      platformFee: split.platformFee,
      creatorShare: split.creatorShare,
      eventType,
      paymentStatus,
      destinationAccountNumber: destinationAccountNumber || undefined,
      destinationBankName: destinationBankName || undefined,
      monnifyTransactionReference: transactionReference || undefined,
      monnifyPaymentReference: paymentReference || undefined,
    })

    io.to(getCreatorRoom(creator._id)).emit("newDonation", donation)

    await createAuditLog({
      actorType: "system",
      eventType: "donation.received",
      message: `Donation received from ${senderDisplay.name}.`,
      metadata: {
        sender: senderDisplay.name,
        senderNameSource: senderDisplay.source,
        amount: split.gross,
        platformFee: split.platformFee,
        creatorShare: split.creatorShare,
        creatorId: creator._id.toString(),
        paymentStatus,
        transactionReference,
        paymentReference,
      },
    })

    return res.sendStatus(200)
  } catch (error) {
    console.error(error)
    return res.status(500).json({ error: "Failed to process Monnify webhook." })
  }
})

app.post("/monnify/test-donation", requireSessionUser, async (req, res) => {
  try {
    if (!isMonnifySandbox()) {
      return res.status(403).json({
        error: "Test donations are only available while Monnify is set to sandbox mode.",
      })
    }

    if (!req.user.virtualAccount?.accountNumber || req.user.virtualAccount.status !== "active") {
      return res.status(400).json({
        error: "This creator does not have an active Monnify virtual account yet.",
      })
    }

    const amount = Number(req.body?.amount) || 0
    const sender =
      sanitizeDonorDisplayName(req.body?.sender || "Monnify Sandbox Tester") ||
      "Monnify Sandbox Tester"

    if (!amount || amount <= 0) {
      return res.status(400).json({ error: "Enter a valid test donation amount." })
    }

    const split = calculateRevenueSplit(amount)
    const uniqueSuffix = `${Date.now()}-${Math.round(Math.random() * 1_000_000)}`
    const donation = await Donation.create({
      creatorId: req.user._id,
      creatorEmail: req.user.email,
      sender,
      senderNameSource: "manual_test",
      amount: split.gross,
      platformFee: split.platformFee,
      creatorShare: split.creatorShare,
      eventType: "monnify.test_api",
      paymentStatus: "PAID",
      destinationAccountNumber: req.user.virtualAccount.accountNumber,
      destinationBankName: req.user.virtualAccount.bankName || "Monnify",
      monnifyTransactionReference: `TEST-TXN-${uniqueSuffix}`,
      monnifyPaymentReference: `TEST-PAY-${uniqueSuffix}`,
      date: new Date(),
    })

    io.to(getCreatorRoom(req.user._id)).emit("newDonation", donation)

    await createAuditLog({
      actorType: "user",
      actorId: req.user._id.toString(),
      eventType: "monnify.test_donation.created",
      message: `${req.user.email} triggered a Monnify sandbox donation test.`,
      metadata: {
        amount: split.gross,
        sender,
        destinationAccountNumber: req.user.virtualAccount.accountNumber,
      },
    })

    return res.status(201).json({
      donation,
      mode: "sandbox",
      message: "Sandbox donation created successfully.",
    })
  } catch (error) {
    console.error(error)
    return res.status(500).json({ error: "Failed to create Monnify sandbox donation." })
  }
})

app.get("/overlay-state", requireSessionUser, async (req, res) => {
  try {
    return res.json(getOverlayStateForUser(req.user))
  } catch (error) {
    console.error(error)
    return res.status(500).json({ error: "Failed to load overlay state." })
  }
})

app.put("/overlay-state", requireSessionUser, async (req, res) => {
  try {
    const currentState = getOverlayStateForUser(req.user)
    const nextState = {
      settings: req.body?.settings ? req.body.settings : currentState.settings,
      customization: req.body?.customization ? req.body.customization : currentState.customization,
      customGifts: Array.isArray(req.body?.customGifts) ? req.body.customGifts : currentState.customGifts,
      leaderboardResetAt:
        typeof req.body?.leaderboardResetAt === "string" && req.body.leaderboardResetAt
          ? new Date(req.body.leaderboardResetAt)
          : currentState.leaderboardResetAt
            ? new Date(currentState.leaderboardResetAt)
            : undefined,
      updatedAt: new Date(),
    }

    req.user.overlayState = nextState
    await req.user.save()

    return res.json(getOverlayStateForUser(req.user))
  } catch (error) {
    console.error(error)
    return res.status(500).json({ error: "Failed to save overlay state." })
  }
})

app.get("/public/overlay/:creatorId", async (req, res) => {
  try {
    const creatorId = String(req.params.creatorId || "").trim()

    if (!mongoose.Types.ObjectId.isValid(creatorId)) {
      return res.status(404).json({ error: "Overlay not found." })
    }

    const user = await User.findById(creatorId)

    if (!user || (user.status && user.status !== "active")) {
      return res.status(404).json({ error: "Overlay not found." })
    }

    const overlayState = getOverlayStateForUser(user)
    const leaderboardResetAt = overlayState.leaderboardResetAt
      ? new Date(overlayState.leaderboardResetAt)
      : null
    const donationQuery = {
      creatorId: user._id,
      ...(leaderboardResetAt && !Number.isNaN(leaderboardResetAt.getTime())
        ? { date: { $gte: leaderboardResetAt } }
        : {}),
    }
    const donations = await Donation.find(donationQuery).sort({ date: -1 }).limit(100)

    res.set("Cache-Control", "no-store, max-age=0")
    return res.json({
      ...overlayState,
      user: sanitizePublicOverlayUser(user),
      donations,
    })
  } catch (error) {
    console.error(error)
    return res.status(500).json({ error: "Failed to load public overlay." })
  }
})

app.get("/payouts", requireSessionUser, async (req, res) => {
  try {
    const payouts = await Payout.find({ creatorId: req.user._id }).sort({ createdAt: -1 })
    res.json(payouts)
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Server error" })
  }
})

app.get("/banks", requireSessionUser, async (_req, res) => {
  try {
    const banks = await getSupportedBanks()
    res.json({ banks })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to load supported banks." })
  }
})

app.post("/bank-account-name-enquiry", requireSessionUser, async (req, res) => {
  try {
    const accountNumber = String(req.body?.accountNumber || "").trim()
    const bankCode = String(req.body?.bankCode || "").trim()

    if (!bankCode || !accountNumber) {
      return res.status(400).json({ error: "Bank code and account number are required." })
    }

    if (accountNumber.length !== 10) {
      return res.status(400).json({ error: "Account number must be 10 digits." })
    }

    const result = await resolveBankAccountName({ bankCode, accountNumber })
    res.json(result)
  } catch (error) {
    console.error(error)
    res.status(400).json({
      error:
        error instanceof Error ? error.message : "Could not resolve the account name for that bank account.",
    })
  }
})

app.post("/payouts", requireSessionUser, async (req, res) => {
  try {
    const { amount, bankName, bankCode = "", accountNumber, accountName = "" } = req.body
    const payoutAmount = Number(amount) || 0

    const creatorTotals = await getRevenueTotals({ creatorId: req.user._id })
    const availableCreatorBalance = creatorTotals.creatorAvailableBalance

    if (!payoutAmount || payoutAmount <= 0) {
      return res.status(400).json({ error: "Enter a valid payout amount." })
    }

    if (payoutAmount > availableCreatorBalance) {
      return res.status(400).json({
        error: "Creators can only withdraw up to their 80% share of earnings.",
        availableCreatorBalance,
        creatorRevenue: creatorTotals.creatorRevenue,
        platformRevenue: creatorTotals.platformRevenue,
      })
    }

    if (!bankName || !accountNumber || !accountName) {
      return res.status(400).json({
        error: "Bank name, account number, and account name are required.",
      })
    }

    if (!bankCode) {
      return res.status(400).json({
        error: "Bank code is required for live disbursement.",
      })
    }

    const transferReference = createTransferReference()
    let transferResponse

    try {
      transferResponse = await initiateMonnifyDisbursement({
        amount: payoutAmount,
        bankCode,
        accountNumber,
        accountName,
        narration: `StreamTip creator payout for ${req.user.name}`,
        reference: transferReference,
      })
    } catch (error) {
      const message = getAxiosErrorMessage(error, "Failed to initiate payout with Monnify.")
      const failedPayout = await Payout.create({
        creatorId: req.user._id,
        amount: payoutAmount,
        bankName,
        bankCode,
        accountNumber,
        accountName,
        status: "failed",
        transferReference,
        providerReference: transferReference,
        providerMessage: message,
        createdAt: new Date(),
        completedAt: new Date(),
      })

      io.to(getCreatorRoom(req.user._id)).emit("newPayout", failedPayout)

      await createAuditLog({
        actorType: "system",
        eventType: "payout.failed",
        message: `Payout failed for ${bankName}.`,
        metadata: {
          amount: payoutAmount,
          bankName,
          bankCode,
          accountNumber: String(accountNumber).slice(-4),
          accountName,
          creatorId: req.user._id.toString(),
          transferReference,
          error: message,
        },
      })

      return res.status(502).json({ error: message, payout: failedPayout })
    }

    const providerStatus =
      transferResponse.status ||
      transferResponse.paymentStatus ||
      transferResponse.transactionStatus ||
      ""
    const payoutStatus = normalizeMonnifyTransferStatus(providerStatus)

    const payout = await Payout.create({
      creatorId: req.user._id,
      amount: payoutAmount,
      bankName,
      bankCode,
      accountNumber,
      accountName,
      status: payoutStatus,
      transferReference,
      providerReference:
        String(
          transferResponse.transactionReference ||
            transferResponse.reference ||
            transferResponse.paymentReference ||
            transferReference,
        ).trim() || transferReference,
      providerMessage: String(
        transferResponse.message || transferResponse.responseMessage || providerStatus || "",
      ).trim(),
      createdAt: new Date(),
      completedAt: payoutStatus === "completed" ? new Date() : undefined,
    })

    io.to(getCreatorRoom(req.user._id)).emit("newPayout", payout)

    await createAuditLog({
      actorType: "system",
      eventType: "payout.created",
      message: `Payout created for ${bankName}.`,
      metadata: {
        amount: payoutAmount,
        bankName,
        bankCode,
        accountNumber: String(accountNumber).slice(-4),
        accountName,
        creatorId: req.user._id.toString(),
        transferReference,
        providerReference: payout.providerReference || "",
        providerStatus: payout.status,
      },
    })

    res.json(payout)
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed payout" })
  }
})

app.get("/donations", requireSessionUser, async (req, res) => {
  const donations = await Donation.find({ creatorId: req.user._id }).sort({ date: -1 })
  res.json(donations)
})

app.get("/user", requireSessionUser, async (req, res) => {
  try {
    res.json(sanitizeUser(req.user))
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to fetch user." })
  }
})

app.put("/user", requireSessionUser, async (req, res) => {
  try {
    const user = req.user

    const nextName = String(req.body?.name || "").trim()
    const nextEmail = String(req.body?.email || "").toLowerCase().trim()
    const nextProfileImage = String(req.body?.profileImage || "").trim()
    const currentPassword = String(req.body?.currentPassword || "")
    const newPassword = String(req.body?.newPassword || "")
    const nextBvn = typeof req.body?.bvn === "string" ? req.body.bvn.trim() : undefined
    const nextNin = typeof req.body?.nin === "string" ? req.body.nin.trim() : undefined

    if (!nextName || !nextEmail) {
      return res.status(400).json({ error: "Name and email are required." })
    }

    const existingUser = await User.findOne({
      email: nextEmail,
      _id: { $ne: user._id },
    })

    if (existingUser) {
      return res.status(409).json({ error: "That email is already in use." })
    }

    user.name = nextName
    user.email = nextEmail
    user.profileImage = nextProfileImage
    user.identity = user.identity || { bvn: "", nin: "" }

    if (nextBvn !== undefined && nextBvn !== "") {
      if (!/^\d{11}$/.test(nextBvn)) {
        return res.status(400).json({ error: "BVN must be 11 digits." })
      }

      user.identity.bvn = nextBvn
    }

    if (nextNin !== undefined && nextNin !== "") {
      if (!/^\d{11}$/.test(nextNin)) {
        return res.status(400).json({ error: "NIN must be 11 digits." })
      }

      user.identity.nin = nextNin
    }

    if (user.virtualAccount) {
      user.virtualAccount.accountName = user.virtualAccount.accountName || `StreamTip/${nextName}`
    }

    if (newPassword) {
      if (newPassword.length < 8) {
        return res.status(400).json({ error: "New password must be at least 8 characters long." })
      }

      if (!currentPassword || !verifyPassword(currentPassword, user.passwordHash)) {
        return res.status(400).json({ error: "Current password is incorrect." })
      }

      user.passwordHash = hashPassword(newPassword)
      user.sessionToken = generateSessionToken()
    }

    await user.save()

    await createAuditLog({
      actorType: "user",
      actorId: user._id.toString(),
      eventType: "user.updated",
      message: `${user.email} updated profile details.`,
      metadata: {
        name: user.name,
        email: user.email,
      },
    })

    res.json(sanitizeUser(user))
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to update user." })
  }
})

app.get("/admin/overview", requireAdminSession, async (req, res) => {
  try {
    const [users, donations, payouts, logs, platformWithdrawals, revenueTotals] = await Promise.all([
      User.find().sort({ createdAt: -1 }),
      Donation.find().sort({ date: -1 }),
      Payout.find().sort({ createdAt: -1 }),
      AuditLog.find().sort({ createdAt: -1 }).limit(100),
      PlatformWithdrawal.find().sort({ createdAt: -1 }),
      getRevenueTotals(),
    ])

    const topGiftersMap = new Map()
    for (const donation of donations) {
      const sender = donation.sender || "Anonymous"
      topGiftersMap.set(sender, (topGiftersMap.get(sender) || 0) + (Number(donation.amount) || 0))
    }

    res.json({
      metrics: {
        totalUsers: users.length,
        totalDonations: donations.length,
        totalPayouts: payouts.length,
        grossRevenue: revenueTotals.grossRevenue,
        platformRevenue: revenueTotals.platformRevenue,
        creatorRevenue: revenueTotals.creatorRevenue,
        creatorAvailableBalance: revenueTotals.creatorAvailableBalance,
        totalPaidOut: revenueTotals.totalPaidOut,
        pendingPlatformRevenue: revenueTotals.pendingPlatformRevenue,
        totalPlatformWithdrawn: revenueTotals.totalPlatformWithdrawn,
      },
      recentUsers: users.slice(0, 10).map(sanitizeUser),
      recentDonations: donations.slice(0, 15),
      recentPayouts: payouts.slice(0, 15),
      recentPlatformWithdrawals: platformWithdrawals.slice(0, 15),
      topGifters: Array.from(topGiftersMap.entries())
        .map(([name, amount]) => ({ name, amount }))
        .sort((a, b) => b.amount - a.amount)
        .slice(0, 8),
      logs,
    })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to load admin overview." })
  }
})

app.get("/admin/logs", requireAdminSession, async (req, res) => {
  try {
    const logs = await AuditLog.find().sort({ createdAt: -1 }).limit(200)
    res.json({ logs })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to load admin logs." })
  }
})

app.get("/admin/users", requireAdminSession, async (req, res) => {
  try {
    const users = await User.find().sort({ createdAt: -1 })
    res.json({ users: users.map(sanitizeUser) })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to load users." })
  }
})

app.delete("/admin/users/:id", requireAdminSession, async (req, res) => {
  try {
    const user = await User.findById(req.params.id)

    if (!user) {
      return res.status(404).json({ error: "User not found." })
    }

    await Promise.all([
      Donation.deleteMany({ creatorId: user._id }),
      Payout.deleteMany({ creatorId: user._id }),
      User.deleteOne({ _id: user._id }),
    ])

    await createAuditLog({
      actorType: "admin",
      actorId: req.adminSession._id.toString(),
      eventType: "admin.user.deleted",
      message: `Admin deleted ${user.email}.`,
      metadata: {
        userId: user._id.toString(),
        email: user.email,
      },
    })

    res.json({ success: true })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to delete user." })
  }
})

app.post("/admin/users", requireAdminSession, async (req, res) => {
  try {
    const {
      name,
      email,
      password,
      role = "creator",
      status = "active",
      provisionVirtualAccount = false,
    } = req.body || {}

    const normalizedEmail = String(email || "").toLowerCase().trim()
    const trimmedName = String(name || "").trim()
    const rawPassword = String(password || "")
    const nextRole = role === "admin" ? "admin" : "creator"
    const nextStatus = ["active", "suspended", "banned"].includes(String(status))
      ? String(status)
      : "active"

    if (!trimmedName || !normalizedEmail || rawPassword.length < 8) {
      return res.status(400).json({
        error: "Name, email, and a password of at least 8 characters are required.",
      })
    }

    const existingUser = await User.findOne({ email: normalizedEmail })
    if (existingUser) {
      return res.status(409).json({ error: "A user with that email already exists." })
    }

    const user = await User.create({
      name: trimmedName,
      email: normalizedEmail,
      passwordHash: hashPassword(rawPassword),
      sessionToken: null,
      role: nextRole,
      status: nextStatus,
    })

    if (provisionVirtualAccount && isMonnifyConfigured()) {
      try {
        await createReservedAccountForUser(user)
      } catch (error) {
        console.error(error)
      }
    }

    await createAuditLog({
      actorType: "admin",
      actorId: req.adminSession._id.toString(),
      eventType: "admin.user.created",
      message: `Admin created user ${normalizedEmail}.`,
      metadata: {
        userId: user._id.toString(),
        email: normalizedEmail,
        role: nextRole,
        status: nextStatus,
      },
    })

    res.status(201).json({ user: sanitizeUser(user) })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to create user." })
  }
})

app.patch("/admin/users/:id", requireAdminSession, async (req, res) => {
  try {
    const user = await User.findById(req.params.id)

    if (!user) {
      return res.status(404).json({ error: "User not found." })
    }

    const nextName = typeof req.body?.name === "string" ? req.body.name.trim() : user.name
    const nextEmail =
      typeof req.body?.email === "string" ? req.body.email.toLowerCase().trim() : user.email
    const nextRole = req.body?.role === "admin" ? "admin" : req.body?.role === "creator" ? "creator" : user.role
    const nextStatus = ["active", "suspended", "banned"].includes(String(req.body?.status))
      ? String(req.body.status)
      : user.status

    if (!nextName || !nextEmail) {
      return res.status(400).json({ error: "Name and email are required." })
    }

    const existingUser = await User.findOne({
      email: nextEmail,
      _id: { $ne: user._id },
    })

    if (existingUser) {
      return res.status(409).json({ error: "Another user already uses that email." })
    }

    user.name = nextName
    user.email = nextEmail
    user.role = nextRole
    user.status = nextStatus

    if (typeof req.body?.password === "string" && req.body.password.trim().length >= 8) {
      user.passwordHash = hashPassword(req.body.password.trim())
      user.sessionToken = null
    }

    await user.save()

    await createAuditLog({
      actorType: "admin",
      actorId: req.adminSession._id.toString(),
      eventType: "admin.user.updated",
      message: `Admin updated user ${user.email}.`,
      metadata: {
        userId: user._id.toString(),
        role: user.role,
        status: user.status,
      },
    })

    res.json({ user: sanitizeUser(user) })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to update user." })
  }
})

app.post("/admin/users/:id/virtual-account", requireAdminSession, async (req, res) => {
  try {
    const user = await User.findById(req.params.id)

    if (!user) {
      return res.status(404).json({ error: "User not found." })
    }

    const virtualAccount = await createReservedAccountForUser(user)

    await createAuditLog({
      actorType: "admin",
      actorId: req.adminSession._id.toString(),
      eventType: "admin.user.virtual_account.created",
      message: `Admin provisioned a virtual account for ${user.email}.`,
      metadata: {
        userId: user._id.toString(),
        email: user.email,
      },
    })

    res.json({ user: sanitizeUser(user), virtualAccount })
  } catch (error) {
    console.error(error)
    res.status(502).json({
      error:
        error instanceof Error
          ? error.message
          : "Could not provision a Monnify reserved account.",
    })
  }
})

app.get("/admin/banks", requireAdminSession, async (_req, res) => {
  try {
    const banks = await getSupportedBanks()
    res.json({ banks })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to load supported banks." })
  }
})

app.post("/admin/bank-account-name-enquiry", requireAdminSession, async (req, res) => {
  try {
    const accountNumber = String(req.body?.accountNumber || "").trim()
    const bankCode = String(req.body?.bankCode || "").trim()

    if (!bankCode || !accountNumber) {
      return res.status(400).json({ error: "Bank code and account number are required." })
    }

    if (accountNumber.length !== 10) {
      return res.status(400).json({ error: "Account number must be 10 digits." })
    }

    const result = await resolveBankAccountName({ bankCode, accountNumber })
    res.json(result)
  } catch (error) {
    console.error(error)
    res.status(400).json({
      error:
        error instanceof Error ? error.message : "Could not resolve the account name for that bank account.",
    })
  }
})

app.get("/admin/platform-withdrawals", requireAdminSession, async (req, res) => {
  try {
    const [withdrawals, totals] = await Promise.all([
      PlatformWithdrawal.find().sort({ createdAt: -1 }),
      getRevenueTotals(),
    ])

    res.json({
      withdrawals,
      history: groupTransactionsByPeriod(withdrawals),
      balance: {
        platformRevenue: totals.platformRevenue,
        totalPlatformWithdrawn: totals.totalPlatformWithdrawn,
        pendingPlatformRevenue: totals.pendingPlatformRevenue,
      },
    })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to load platform withdrawals." })
  }
})

app.post("/admin/platform-withdrawals", requireAdminSession, async (req, res) => {
  try {
    const { amount, bankName, bankCode = "", accountNumber, accountName, note = "" } = req.body || {}
    const withdrawalAmount = Number(amount) || 0
    const totals = await getRevenueTotals()

    if (!withdrawalAmount || withdrawalAmount <= 0) {
      return res.status(400).json({ error: "Enter a valid withdrawal amount." })
    }

    if (!bankName || !accountNumber || !accountName) {
      return res.status(400).json({
        error: "Bank name, account number, and account name are required.",
      })
    }

    if (withdrawalAmount > totals.pendingPlatformRevenue) {
      return res.status(400).json({
        error: "You can only withdraw from the available platform 20% balance.",
        pendingPlatformRevenue: totals.pendingPlatformRevenue,
      })
    }

    const transferReference = createTransferReference("STIP-PLATFORM")
    const withdrawal = await PlatformWithdrawal.create({
      amount: withdrawalAmount,
      bankName: String(bankName).trim(),
      bankCode: String(bankCode).trim(),
      accountNumber: String(accountNumber).trim(),
      accountName: String(accountName).trim(),
      note: String(note || "").trim(),
      status: "recorded",
      transferReference,
      providerReference: transferReference,
      providerMessage: "Manual platform withdrawal recorded by admin.",
      createdAt: new Date(),
      completedAt: new Date(),
    })

    await createAuditLog({
      actorType: "admin",
      actorId: req.adminSession._id.toString(),
      eventType: "admin.platform_withdrawal.created",
      message: `Admin recorded a manual platform withdrawal to ${withdrawal.bankName}.`,
      metadata: {
        amount: withdrawalAmount,
        bankName: withdrawal.bankName,
        bankCode: withdrawal.bankCode || "",
        accountNumber: withdrawal.accountNumber.slice(-4),
        accountName: withdrawal.accountName,
        transferReference,
        providerReference: withdrawal.providerReference || "",
        providerStatus: withdrawal.status,
        mode: "manual",
      },
    })

    const refreshedTotals = await getRevenueTotals()

    res.status(201).json({
      withdrawal,
      balance: {
        platformRevenue: refreshedTotals.platformRevenue,
        totalPlatformWithdrawn: refreshedTotals.totalPlatformWithdrawn,
        pendingPlatformRevenue: refreshedTotals.pendingPlatformRevenue,
      },
    })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to create platform withdrawal." })
  }
})

const PORT = Number(process.env.PORT || 5000)

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Server running on http://0.0.0.0:${PORT}`)
})
