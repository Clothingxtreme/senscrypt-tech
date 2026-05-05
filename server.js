const express = require("express")
const http = require("http")
const mongoose = require("mongoose")
const cors = require("cors")
const axios = require("axios")
const crypto = require("crypto")
const fs = require("fs")
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
const MONGODB_DATABASE = readEnv("MONGODB_DATABASE") || "streamtip"
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
const TELEGRAM_BOT_TOKEN = readEnv("TELEGRAM_BOT_TOKEN")
const TELEGRAM_WITHDRAWAL_CHAT_ID =
  readEnv("TELEGRAM_WITHDRAWAL_CHAT_ID") || readEnv("TELEGRAM_CHAT_ID")
const PORTAL_URL = readEnv("PORTAL_URL") || "http://localhost:5001"
const MONNIFY_DISBURSEMENT_SOURCE_ACCOUNT_NUMBER = readEnv(
  "MONNIFY_DISBURSEMENT_SOURCE_ACCOUNT_NUMBER",
)

const missingRequiredEnv = ["MONGODB_URI"].filter((key) => !process.env[key])
if (missingRequiredEnv.length > 0) {
  console.error(`Missing required environment variables: ${missingRequiredEnv.join(", ")}`)
}

function parseAllowedOrigins(value) {
  const localOrigins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:3001",
    "http://127.0.0.1:3001",
    "http://localhost:5001",
    "http://127.0.0.1:5001",
  ]
  const configuredOrigins = String(value || "")
    .split(",")
    .map((origin) => origin.trim())
    .filter(Boolean)

  if (configuredOrigins.includes("*")) {
    return "*"
  }

  return Array.from(new Set([...configuredOrigins, ...localOrigins]))
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

function getPublicRequestBaseUrl(req) {
  const forwardedProto = String(req.headers["x-forwarded-proto"] || "").split(",")[0].trim()
  const protocol = forwardedProto || req.protocol || "http"
  const host = req.get("host")

  return `${protocol}://${host}`
}

function getAudioUploadExtension(mimeType) {
  const normalized = String(mimeType || "").toLowerCase().split(";")[0].trim()

  if (normalized === "audio/mpeg" || normalized === "audio/mp3") return ".mp3"
  if (normalized === "audio/wav" || normalized === "audio/x-wav" || normalized === "audio/wave") {
    return ".wav"
  }
  if (normalized === "audio/ogg") return ".ogg"
  if (normalized === "audio/webm") return ".webm"
  if (normalized === "audio/aac") return ".aac"
  if (normalized === "audio/mp4" || normalized === "audio/x-m4a") return ".m4a"

  return ""
}

function getAudioUploadExtensionFromName(fileName) {
  const extension = path.extname(String(fileName || "").toLowerCase())

  if ([".mp3", ".wav", ".ogg", ".webm", ".aac", ".m4a"].includes(extension)) {
    return extension
  }

  return ""
}

async function saveGiftSoundUpload({ req, buffer, mimeType, originalFileName }) {
  const extension = getAudioUploadExtension(mimeType) || getAudioUploadExtensionFromName(originalFileName)

  if (!extension) {
    const error = new Error("Upload an MP3, WAV, OGG, WEBM, AAC, or M4A audio file.")
    error.statusCode = 400
    throw error
  }

  if (!Buffer.isBuffer(buffer) || buffer.length === 0) {
    const error = new Error("No audio file was uploaded.")
    error.statusCode = 400
    throw error
  }

  if (buffer.length > 2 * 1024 * 1024) {
    const error = new Error("Use an audio file smaller than 2 MB.")
    error.statusCode = 400
    throw error
  }

  const sound = await GiftSound.create({
    ownerId: req.user._id,
    fileName: String(originalFileName || "Uploaded audio").slice(0, 80),
    contentType: String(mimeType || "audio/mpeg").split(";")[0].trim(),
    data: buffer,
    createdAt: new Date(),
  })

  return {
    soundUrl: `${getPublicRequestBaseUrl(req)}/uploads/gift-sounds/${sound._id}`,
    soundName: String(originalFileName || "Uploaded audio").slice(0, 80),
  }
}

async function normalizeCustomGiftSoundUploads(req, customGifts) {
  if (!Array.isArray(customGifts)) {
    return customGifts
  }

  return Promise.all(
    customGifts.map(async (gift) => {
      const soundUrl = String(gift?.soundUrl || "")
      const match = soundUrl.match(/^data:([^;]+);base64,(.+)$/)

      if (!match) {
        return gift
      }

      const savedSound = await saveGiftSoundUpload({
        req,
        buffer: Buffer.from(match[2], "base64"),
        mimeType: match[1],
        originalFileName: gift.soundName || `${gift.name || "gift"} sound`,
      })

      return {
        ...gift,
        soundUrl: savedSound.soundUrl,
        soundName: savedSound.soundName,
      }
    }),
  )
}

const app = express()
app.set("trust proxy", true)

const uploadsRoot = path.join(__dirname, "uploads")
const giftSoundsUploadDir = path.join(uploadsRoot, "gift-sounds")
fs.mkdirSync(giftSoundsUploadDir, { recursive: true })

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
app.use("/uploads", express.static(uploadsRoot))
app.use(
  express.json({
    limit: "25mb",
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
const MIN_CREATOR_WITHDRAWAL = 5000
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

function getMongoUriDatabaseName() {
  if (!MONGODB_URI) {
    return ""
  }

  try {
    const pathname = new URL(MONGODB_URI).pathname.replace(/^\/+/, "").trim()
    return pathname ? decodeURIComponent(pathname.split("/")[0]) : ""
  } catch (_error) {
    return ""
  }
}

function getMongoConnectionOptions() {
  const options = { serverSelectionTimeoutMS: 15000 }

  if (!getMongoUriDatabaseName() && MONGODB_DATABASE) {
    options.dbName = MONGODB_DATABASE
  }

  return options
}

if (MONGODB_URI) {
  mongoose
    .connect(MONGODB_URI, getMongoConnectionOptions())
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

  console.error("database.not_ready", {
    readyState: mongoose.connection.readyState,
    host: getMongoUriHost(),
    databaseName: mongoose.connection.name || "",
    lastDatabaseError,
  })

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
  paymentMethod: String,
  currency: String,
  paidOn: Date,
  sourceAccountName: String,
  sourceAccountNumber: String,
  sourceBankName: String,
  sourceBankCode: String,
  sourceSessionId: String,
  destinationAccountNumber: String,
  destinationBankName: String,
  destinationBankCode: String,
  monnifyTransactionReference: { type: String, index: true, sparse: true },
  monnifyPaymentReference: { type: String, index: true, sparse: true },
  providerPayload: { type: mongoose.Schema.Types.Mixed, default: undefined },
  date: { type: Date, default: Date.now },
})

donationSchema.index({ sourceSessionId: 1 }, { sparse: true })
donationSchema.index({ sourceAccountNumber: 1 }, { sparse: true })
donationSchema.index({ destinationAccountNumber: 1 }, { sparse: true })
donationSchema.index({ date: -1 })

const Donation = mongoose.model("Donation", donationSchema)

const complianceInflowStatuses = ["held", "unmatched", "paid", "reversed", "resolved"]

const complianceInflowSchema = new mongoose.Schema({
  sourceAccountName: String,
  sourceAccountNumber: String,
  sourceBankName: String,
  sourceBankCode: String,
  sourceSessionId: String,
  amount: Number,
  currency: String,
  paymentStatus: String,
  paymentMethod: String,
  eventType: String,
  paidOn: Date,
  destinationAccountNumber: String,
  destinationBankName: String,
  destinationBankCode: String,
  reservedAccountReference: String,
  monnifyTransactionReference: { type: String, index: true, sparse: true },
  monnifyPaymentReference: { type: String, index: true, sparse: true },
  rawMonnifyPayload: { type: mongoose.Schema.Types.Mixed, default: undefined },
  status: { type: String, enum: complianceInflowStatuses, default: "held", index: true },
  validationReason: String,
  adminNotes: String,
  linkedCreatorId: { type: mongoose.Schema.Types.ObjectId, ref: "User", index: true },
  linkedCreatorEmail: String,
  linkedDonationId: { type: mongoose.Schema.Types.ObjectId, ref: "Donation", index: true },
  resolvedBy: String,
  resolvedAt: Date,
  date: { type: Date, default: Date.now, index: true },
  updatedAt: Date,
})

complianceInflowSchema.index({ sourceSessionId: 1 }, { sparse: true })
complianceInflowSchema.index({ sourceAccountNumber: 1 }, { sparse: true })
complianceInflowSchema.index({ destinationAccountNumber: 1 }, { sparse: true })
complianceInflowSchema.index({ reservedAccountReference: 1 }, { sparse: true })

const ComplianceInflow = mongoose.model("ComplianceInflow", complianceInflowSchema)

const payoutSchema = new mongoose.Schema({
  creatorId: { type: mongoose.Schema.Types.ObjectId, ref: "User", index: true },
  amount: Number,
  status: String,
  reviewStatus: { type: String, default: "not_required", index: true },
  reviewReason: String,
  reviewedBy: String,
  reviewedAt: Date,
  rejectionReason: String,
  bankName: String,
  bankCode: String,
  accountNumber: String,
  accountName: String,
  transferReference: String,
  providerReference: String,
  providerMessage: String,
  previousTransferReferences: { type: [String], default: undefined },
  retryCount: { type: Number, default: 0 },
  createdAt: { type: Date, default: Date.now },
  completedAt: Date,
})

const Payout = mongoose.model("Payout", payoutSchema)

const payoutProfileChangeRequestSchema = new mongoose.Schema(
  {
    creatorId: { type: mongoose.Schema.Types.ObjectId, ref: "User", index: true },
    currentProfile: { type: Object, default: {} },
    requestedProfile: { type: Object, default: {} },
    supportNote: String,
    proofSummary: String,
    status: { type: String, default: "awaiting_review", index: true },
    reviewedBy: String,
    reviewedAt: Date,
    rejectionReason: String,
    cooldownUntil: Date,
    createdAt: { type: Date, default: Date.now },
  },
  { timestamps: false },
)

const PayoutProfileChangeRequest = mongoose.model(
  "PayoutProfileChangeRequest",
  payoutProfileChangeRequestSchema,
)

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

const giftSoundSchema = new mongoose.Schema(
  {
    ownerId: { type: mongoose.Schema.Types.ObjectId, ref: "User", index: true },
    fileName: { type: String, default: "Uploaded audio" },
    contentType: { type: String, default: "audio/mpeg" },
    data: Buffer,
    createdAt: { type: Date, default: Date.now },
  },
  { timestamps: false },
)

const AdminSession = mongoose.model("AdminSession", adminSessionSchema)
const AuditLog = mongoose.model("AuditLog", auditLogSchema)
const GiftSound = mongoose.model("GiftSound", giftSoundSchema)

const userSchema = new mongoose.Schema(
  {
    name: { type: String, required: true },
    firstName: { type: String, default: "" },
    middleName: { type: String, default: "" },
    lastName: { type: String, default: "" },
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
      dateOfBirth: { type: String, default: "" },
      firstName: { type: String, default: "" },
      middleName: { type: String, default: "" },
      lastName: { type: String, default: "" },
      policyAcceptedAt: Date,
      signupCompletedAt: Date,
    },
    payoutProfile: {
      bankName: String,
      bankCode: String,
      accountNumber: String,
      accountName: String,
      firstName: String,
      middleName: String,
      lastName: String,
      status: { type: String, default: "missing" },
      locked: { type: Boolean, default: false },
      verifiedAt: Date,
      lockedAt: Date,
      changeRequiresSupport: { type: Boolean, default: true },
      verificationProvider: { type: String, default: "monnify" },
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

function splitNamePartsFromFullName(name) {
  const parts = String(name || "").trim().split(/\s+/).filter(Boolean)

  return {
    firstName: parts[0] || "",
    middleName: parts.length > 2 ? parts.slice(1, -1).join(" ") : "",
    lastName: parts.length > 1 ? parts[parts.length - 1] : "",
  }
}

function buildFullNameFromParts({ firstName, middleName, lastName }, fallback = "") {
  return [firstName, middleName, lastName]
    .map((part) => String(part || "").trim())
    .filter(Boolean)
    .join(" ")
    .trim() || String(fallback || "Creator").trim()
}

function getUserNameParts(user) {
  const fallbackParts = splitNamePartsFromFullName(user?.name)
  const identity = user?.identity || {}

  return {
    firstName: String(user?.firstName || identity.firstName || fallbackParts.firstName || "").trim(),
    middleName: String(user?.middleName || identity.middleName || fallbackParts.middleName || "").trim(),
    lastName: String(user?.lastName || identity.lastName || fallbackParts.lastName || "").trim(),
  }
}

function applyUserNameParts(user, parts) {
  const nextParts = {
    firstName: String(parts?.firstName || "").trim(),
    middleName: String(parts?.middleName || "").trim(),
    lastName: String(parts?.lastName || "").trim(),
  }

  user.firstName = nextParts.firstName
  user.middleName = nextParts.middleName
  user.lastName = nextParts.lastName
  user.name = buildFullNameFromParts(nextParts, user.name || user.email || "Creator")
  user.identity = user.identity || {}
  user.identity.firstName = nextParts.firstName
  user.identity.middleName = nextParts.middleName
  user.identity.lastName = nextParts.lastName

  return nextParts
}

async function backfillUserNameParts() {
  if (!isDatabaseConnected()) return

  const users = await User.find({
    $or: [
      { firstName: { $in: [null, ""] } },
      { lastName: { $in: [null, ""] } },
      { "identity.firstName": { $in: [null, ""] } },
      { "identity.lastName": { $in: [null, ""] } },
    ],
  }).limit(500)

  for (const user of users) {
    const parts = getUserNameParts(user)
    if (!parts.firstName && !parts.lastName) continue

    applyUserNameParts(user, parts)
    await user.save()
  }
}

if (mongoose.connection.readyState === 1) {
  void backfillUserNameParts().catch((error) => console.error("user.name_parts_backfill_failed", error))
} else {
  mongoose.connection.once("open", () => {
    void backfillUserNameParts().catch((error) => console.error("user.name_parts_backfill_failed", error))
  })
}

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

function sanitizeIdentity(identity, fallbackParts = {}) {
  const bvn = String(identity?.bvn || "").trim()
  const nin = String(identity?.nin || "").trim()
  const firstName = String(identity?.firstName || fallbackParts.firstName || "").trim()
  const middleName = String(identity?.middleName || fallbackParts.middleName || "").trim()
  const lastName = String(identity?.lastName || fallbackParts.lastName || "").trim()

  return {
    hasBvn: Boolean(bvn),
    hasNin: Boolean(nin),
    bvnLast4: bvn ? bvn.slice(-4) : "",
    ninLast4: nin ? nin.slice(-4) : "",
    hasDateOfBirth: Boolean(String(identity?.dateOfBirth || "").trim()),
    dateOfBirth: identity?.dateOfBirth || "",
    firstName,
    middleName,
    lastName,
    policyAcceptedAt: identity?.policyAcceptedAt || "",
    signupCompletedAt: identity?.signupCompletedAt || "",
    isComplete: Boolean(
      bvn &&
        nin &&
        String(identity?.dateOfBirth || "").trim() &&
        firstName &&
        lastName,
    ),
  }
}

function sanitizePayoutProfile(profile) {
  if (!profile?.accountNumber) {
    return {
      status: "missing",
      locked: false,
      changeRequiresSupport: true,
    }
  }

  return {
    bankName: profile.bankName || "",
    bankCode: profile.bankCode || "",
    accountNumber: profile.accountNumber || "",
    accountName: profile.accountName || "",
    firstName: profile.firstName || "",
    middleName: profile.middleName || "",
    lastName: profile.lastName || "",
    status: profile.status || "missing",
    locked: Boolean(profile.locked),
    verifiedAt: profile.verifiedAt || "",
    lockedAt: profile.lockedAt || "",
    changeRequiresSupport: profile.changeRequiresSupport !== false,
    verificationProvider: profile.verificationProvider || "monnify",
  }
}

function sanitizePayoutProfileChangeRequest(request) {
  if (!request) return null

  return {
    id: request._id,
    creatorId: request.creatorId?._id || request.creatorId || "",
    creator: request.creatorId?._id ? sanitizeUser(request.creatorId) : undefined,
    currentProfile: sanitizePayoutProfile(request.currentProfile),
    requestedProfile: sanitizePayoutProfile(request.requestedProfile),
    supportNote: request.supportNote || "",
    proofSummary: request.proofSummary || "",
    status: request.status || "awaiting_review",
    reviewedBy: request.reviewedBy || "",
    reviewedAt: request.reviewedAt || "",
    rejectionReason: request.rejectionReason || "",
    cooldownUntil: request.cooldownUntil || "",
    createdAt: request.createdAt || "",
  }
}

function normalizeNameToken(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z]/g, "")
    .trim()
}

function tokenizeName(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z]+/g, " ")
    .trim()
    .split(/\s+/)
    .filter(Boolean)
}

function validateLegalNameParts({ firstName, middleName, lastName }) {
  const first = String(firstName || "").trim()
  const middle = String(middleName || "").trim()
  const last = String(lastName || "").trim()

  if (!first || !last) {
    throw new Error("First name and last name are required.")
  }

  return { first, middle, last }
}

function resolvePayoutNamePart(value, fallback = "") {
  const trimmed = typeof value === "string" ? value.trim() : ""
  return trimmed || String(fallback || "").trim()
}

function validatePayoutAccountNameMatch({
  firstName,
  middleName,
  lastName,
  first,
  middle,
  last,
  accountName,
}) {
  const legal = validateLegalNameParts({
    firstName: resolvePayoutNamePart(firstName, first),
    middleName: resolvePayoutNamePart(middleName, middle),
    lastName: resolvePayoutNamePart(lastName, last),
  })
  const accountTokens = tokenizeName(accountName)
  const missing = [legal.first, legal.middle, legal.last]
    .map((part) => ({ original: part, normalized: normalizeNameToken(part) }))
    .filter((part) => part.normalized && !accountTokens.includes(part.normalized))

  if (missing.length > 0) {
    throw new Error(
      `Bank account name must match your first and last name${legal.middle ? ", plus your middle name" : ""}. Missing match: ${missing
        .map((part) => part.original)
        .join(", ")}. If this is a special case, contact support for manual verification.`,
    )
  }

  return legal
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

function compactString(value) {
  return String(value ?? "").trim()
}

function firstObject(value) {
  if (Array.isArray(value)) {
    return value.find((item) => item && typeof item === "object" && !Array.isArray(item)) || {}
  }

  if (value && typeof value === "object") {
    return value
  }

  return {}
}

function firstStringFromObjects(objects, keys) {
  for (const source of objects) {
    const object = firstObject(source)

    for (const key of keys) {
      const value = compactString(object[key])

      if (value) {
        return value
      }
    }
  }

  return ""
}

function firstStringFromPaths(sources, paths) {
  for (const source of sources) {
    for (const path of paths) {
      const value = compactString(getNestedValue(source, path))

      if (value) {
        return value
      }
    }
  }

  return ""
}

function parseProviderDate(value) {
  const raw = compactString(value)

  if (!raw) {
    return undefined
  }

  const normalized = raw.includes("T") ? raw : raw.replace(" ", "T")
  const date = new Date(normalized)

  if (Number.isNaN(date.getTime())) {
    return undefined
  }

  return date
}

function getPaymentSourceObjects(eventData, data) {
  return [
    eventData?.paymentSourceInformation,
    eventData?.sourceAccountInformation,
    eventData?.originatorAccountInformation,
    eventData?.accountDetails,
    eventData?.accountPayments,
    eventData?.payer,
    data?.paymentSourceInformation,
    data?.sourceAccountInformation,
    data?.originatorAccountInformation,
    data?.accountDetails,
    data?.accountPayments,
    data?.payer,
  ].filter(Boolean)
}

function getDonationSourceDetails(eventData, data) {
  const sourceObjects = getPaymentSourceObjects(eventData, data)
  const sources = [eventData, data]

  return {
    sourceAccountName:
      firstStringFromObjects(sourceObjects, [
        "accountName",
        "accountHolderName",
        "originatorAccountName",
        "sourceAccountName",
        "name",
      ]) ||
      firstStringFromPaths(sources, [
        "originatorAccountName",
        "sourceAccountName",
        "accountDetails.accountName",
        "accountPayments.accountName",
        "payerName",
        "payer.accountName",
        "payer.name",
      ]),
    sourceAccountNumber:
      firstStringFromObjects(sourceObjects, [
        "accountNumber",
        "originatorAccountNumber",
        "sourceAccountNumber",
        "nuban",
      ]) ||
      firstStringFromPaths(sources, [
        "originatorAccountNumber",
        "sourceAccountNumber",
        "accountDetails.accountNumber",
        "accountPayments.accountNumber",
        "payer.accountNumber",
      ]),
    sourceBankName:
      firstStringFromObjects(sourceObjects, [
        "bankName",
        "bank",
        "originatorBankName",
        "sourceBankName",
      ]) ||
      firstStringFromPaths(sources, [
        "originatorBankName",
        "sourceBankName",
        "accountDetails.bankName",
        "accountPayments.bankName",
        "payer.bankName",
      ]),
    sourceBankCode:
      firstStringFromObjects(sourceObjects, [
        "bankCode",
        "originatorBankCode",
        "sourceBankCode",
      ]) ||
      firstStringFromPaths(sources, [
        "originatorBankCode",
        "sourceBankCode",
        "accountDetails.bankCode",
        "accountPayments.bankCode",
        "payer.bankCode",
      ]),
    sourceSessionId:
      firstStringFromObjects(sourceObjects, [
        "sessionId",
        "sessionID",
        "session_id",
        "sourceSessionId",
      ]) ||
      firstStringFromPaths(sources, [
        "sessionId",
        "sessionID",
        "sourceSessionId",
        "accountDetails.sessionId",
        "accountPayments.sessionId",
        "paymentSourceInformation.sessionId",
      ]),
  }
}

function getDonationDestinationDetails(eventData, data) {
  const destinationObjects = [
    eventData?.destinationAccountInformation,
    eventData?.accountDetails,
    eventData?.accountPayments,
    data?.destinationAccountInformation,
    data?.accountDetails,
    data?.accountPayments,
  ].filter(Boolean)
  const sources = [eventData, data]

  return {
    destinationAccountNumber:
      firstStringFromObjects(destinationObjects, ["destinationAccountNumber", "accountNumber"]) ||
      firstStringFromPaths(sources, [
        "destinationAccountNumber",
        "accountDetails.destinationAccountNumber",
        "accountPayments.destinationAccountNumber",
      ]),
    destinationBankName:
      firstStringFromObjects(destinationObjects, ["bankName", "bank", "destinationBankName"]) ||
      firstStringFromPaths(sources, [
        "destinationBankName",
        "accountDetails.destinationBankName",
        "accountPayments.destinationBankName",
      ]),
    destinationBankCode:
      firstStringFromObjects(destinationObjects, ["bankCode", "destinationBankCode"]) ||
      firstStringFromPaths(sources, [
        "destinationBankCode",
        "accountDetails.destinationBankCode",
        "accountPayments.destinationBankCode",
      ]),
  }
}

function getReservedAccountReference(eventData = {}, data = {}) {
  return (
    firstStringFromPaths([eventData, data], [
      "product.reference",
      "product.accountReference",
      "product.reservationReference",
      "accountReference",
      "reservedAccountReference",
      "reservationReference",
      "accountDetails.accountReference",
      "accountPayments.accountReference",
    ]) || ""
  )
}

function getMonnifyPaymentReferences(eventData = {}, data = {}) {
  return {
    transactionReference: compactString(
      eventData.transactionReference ||
        eventData.transactionRef ||
        data.transactionReference ||
        data.transactionRef,
    ),
    paymentReference: compactString(
      eventData.paymentReference ||
        eventData.transactionHash ||
        data.paymentReference ||
        data.transactionHash,
    ),
  }
}

function normalizeComplianceInflowStatus({ paymentStatus, creator, destinationAccountNumber, amount, donation }) {
  const normalizedPaymentStatus = String(paymentStatus || "").toUpperCase()

  if (donation) {
    return "resolved"
  }

  if (/REVERSE|REVERSED|REFUND|REFUNDED|CHARGEBACK/i.test(normalizedPaymentStatus)) {
    return "reversed"
  }

  if (normalizedPaymentStatus === "PAID") {
    if (!creator || !destinationAccountNumber || !(Number(amount) > 0)) {
      return "unmatched"
    }

    return "paid"
  }

  return "held"
}

function getComplianceInflowValidationReason({
  paymentStatus,
  creator,
  destinationAccountNumber,
  amount,
  donation,
  existingDonation,
}) {
  const normalizedPaymentStatus = String(paymentStatus || "").toUpperCase()

  if (donation) {
    return "Resolved into a StreamTip gift donation."
  }

  if (existingDonation) {
    return "Duplicate Monnify webhook; donation already existed."
  }

  if (normalizedPaymentStatus && normalizedPaymentStatus !== "PAID") {
    return `Provider status is ${normalizedPaymentStatus}; waiting for final payment validation.`
  }

  if (!destinationAccountNumber) {
    return "Missing destination virtual account number."
  }

  if (!creator) {
    return "Destination account could not be matched to a StreamTip creator."
  }

  if (!(Number(amount) > 0)) {
    return "Paid webhook had no valid amount."
  }

  return "Paid inflow is pending admin validation."
}

async function upsertComplianceInflowFromMonnify({
  eventType,
  eventData = {},
  data = {},
  creator = null,
  donation = null,
  existingDonation = null,
  status,
  validationReason,
}) {
  const sourceDetails = getDonationSourceDetails(eventData, data)
  const destinationDetails = getDonationDestinationDetails(eventData, data)
  const references = getMonnifyPaymentReferences(eventData, data)
  const paymentStatus = String(
    eventData.paymentStatus || eventData.status || data.paymentStatus || data.status || "PENDING",
  ).toUpperCase()
  const amount = firstValidMoneyAmount(
    eventData.amountPaid,
    eventData.amount,
    eventData.settlementAmount,
    eventData.totalPayable,
    data.amount,
    data.amountPaid,
  )
  const linkedCreator = creator || null
  const linkedDonation = donation || existingDonation || null
  const searchClauses = []

  if (references.transactionReference) {
    searchClauses.push({ monnifyTransactionReference: references.transactionReference })
  }

  if (references.paymentReference) {
    searchClauses.push({ monnifyPaymentReference: references.paymentReference })
  }

  if (sourceDetails.sourceSessionId) {
    searchClauses.push({ sourceSessionId: sourceDetails.sourceSessionId })
  }

  const computedStatus =
    status ||
    normalizeComplianceInflowStatus({
      paymentStatus,
      creator: linkedCreator,
      destinationAccountNumber: destinationDetails.destinationAccountNumber,
      amount,
      donation: linkedDonation,
    })
  const computedReason =
    validationReason ||
    getComplianceInflowValidationReason({
      paymentStatus,
      creator: linkedCreator,
      destinationAccountNumber: destinationDetails.destinationAccountNumber,
      amount,
      donation,
      existingDonation,
    })

  const inflow =
    searchClauses.length > 0 ? await ComplianceInflow.findOne({ $or: searchClauses }) : null
  const record = inflow || new ComplianceInflow({ date: new Date() })
  const linkedCreatorId =
    linkedCreator?._id || linkedDonation?.creatorId || record.linkedCreatorId || undefined

  record.sourceAccountName = sourceDetails.sourceAccountName || record.sourceAccountName
  record.sourceAccountNumber = sourceDetails.sourceAccountNumber || record.sourceAccountNumber
  record.sourceBankName = sourceDetails.sourceBankName || record.sourceBankName
  record.sourceBankCode = sourceDetails.sourceBankCode || record.sourceBankCode
  record.sourceSessionId = sourceDetails.sourceSessionId || record.sourceSessionId
  record.amount = Number(amount) > 0 ? amount : record.amount
  record.currency = compactString(eventData.currency || data.currency || "NGN") || record.currency
  record.paymentStatus = paymentStatus || record.paymentStatus
  record.paymentMethod = compactString(eventData.paymentMethod || data.paymentMethod) || record.paymentMethod
  record.eventType = eventType || record.eventType
  record.paidOn =
    parseProviderDate(eventData.paidOn || eventData.paidAt || data.paidOn || data.paidAt) || record.paidOn
  record.destinationAccountNumber =
    destinationDetails.destinationAccountNumber || record.destinationAccountNumber
  record.destinationBankName = destinationDetails.destinationBankName || record.destinationBankName
  record.destinationBankCode = destinationDetails.destinationBankCode || record.destinationBankCode
  record.reservedAccountReference = getReservedAccountReference(eventData, data) || record.reservedAccountReference
  record.monnifyTransactionReference =
    references.transactionReference || record.monnifyTransactionReference
  record.monnifyPaymentReference = references.paymentReference || record.monnifyPaymentReference
  record.rawMonnifyPayload = data
  record.status = computedStatus
  record.validationReason = computedReason
  record.linkedCreatorId = linkedCreatorId
  record.linkedCreatorEmail =
    linkedCreator?.email || linkedDonation?.creatorEmail || record.linkedCreatorEmail
  record.linkedDonationId = linkedDonation?._id || record.linkedDonationId
  record.updatedAt = new Date()

  if (computedStatus === "resolved" && !record.resolvedAt) {
    record.resolvedAt = new Date()
  }

  await record.save()
  return record
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
  const compact = sanitized.replace(/[^a-z0-9]/gi, "").toLowerCase()

  if (!normalized) return true
  if (creatorNames.includes(normalized)) return true
  if (normalized.startsWith("streamtip ")) return true
  if (/^(mfy|mnfy|monnify|stip|stp|trf|txn|ref)\d{5,}/i.test(compact)) {
    return true
  }
  if (/^(mfy|mnfy|monnify|stip|stp|trf|txn|ref)(\s|\/|-)/i.test(sanitized)) {
    return true
  }
  if ((sanitized.match(/\//g) || []).length >= 2 && /\d{8,}/.test(sanitized)) {
    return true
  }
  if (
    /^(transfer|bank transfer|payment|bank transfer payment|donation|gift|monnify|moniepoint|wallet funding)$/i.test(
      sanitized,
    )
  ) {
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

function collectNestedValuesByKey(source, keyPattern, maxDepth = 5) {
  const results = []
  const visited = new Set()

  function visit(value, depth) {
    if (!value || typeof value !== "object" || depth > maxDepth || visited.has(value)) {
      return
    }

    visited.add(value)

    if (Array.isArray(value)) {
      value.forEach((item) => visit(item, depth + 1))
      return
    }

    Object.entries(value).forEach(([key, nestedValue]) => {
      if (keyPattern.test(key) && typeof nestedValue !== "object") {
        results.push(nestedValue)
      }

      visit(nestedValue, depth + 1)
    })
  }

  visit(source, 0)
  return results
}

function collectNestedEntriesByKey(source, keyPattern, maxDepth = 5) {
  const results = []
  const visited = new Set()

  function visit(value, depth, pathParts) {
    if (!value || typeof value !== "object" || depth > maxDepth || visited.has(value)) {
      return
    }

    visited.add(value)

    if (Array.isArray(value)) {
      value.forEach((item, index) => visit(item, depth + 1, [...pathParts, String(index)]))
      return
    }

    Object.entries(value).forEach(([key, nestedValue]) => {
      const nextPathParts = [...pathParts, key]

      if (keyPattern.test(key) && typeof nestedValue !== "object") {
        const displayValue = sanitizeDonorDisplayName(nestedValue)

        if (displayValue) {
          results.push({
            path: nextPathParts.join("."),
            value: displayValue,
          })
        }
      }

      visit(nestedValue, depth + 1, nextPathParts)
    })
  }

  visit(source, 0, [])
  return results
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
    "paymentDescription",
    "paymentNarration",
    "transactionNarration",
    "transactionDescription",
    "transactionRemark",
    "paymentSourceInformation.narration",
    "paymentSourceInformation.remark",
    "paymentSourceInformation.remarks",
    "paymentSourceInformation.description",
    "paymentSourceInformation.paymentDescription",
    "paymentSourceInformation.transactionDescription",
    "sourceAccountInformation.narration",
    "sourceAccountInformation.remark",
    "sourceAccountInformation.remarks",
    "sourceAccountInformation.description",
    "sourceAccountInformation.paymentDescription",
    "sourceAccountInformation.transactionDescription",
    "originatorNarration",
    "originatorRemark",
    "senderNarration",
    "senderRemark",
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
    ...collectNestedValuesByKey(
      eventData,
      /(narration|remark|remarks|senderremark|originatorremark|description|note|comment)$/i,
    ),
    ...collectNestedValuesByKey(
      data,
      /(narration|remark|remarks|senderremark|originatorremark|description|note|comment)$/i,
    ),
  ]
  const checkedNarrationFields = [
    ...collectNestedEntriesByKey(
      eventData,
      /(narration|remark|remarks|senderremark|originatorremark|description|note|comment|reference)$/i,
    ).map((entry) => ({ ...entry, root: "eventData" })),
    ...collectNestedEntriesByKey(
      data,
      /(narration|remark|remarks|senderremark|originatorremark|description|note|comment|reference)$/i,
    ).map((entry) => ({ ...entry, root: "data" })),
  ].slice(0, 20)
  const checkedNarrations = []

  for (const candidate of narrationCandidates) {
    const nickname = sanitizeDonorDisplayName(candidate)

    if (nickname) {
      checkedNarrations.push(nickname)
    }

    if (!nickname || isSystemNarration(nickname, creatorNames)) {
      continue
    }

    return {
      name: nickname,
      source: "narration",
      checkedNarrations: Array.from(new Set(checkedNarrations)).slice(0, 12),
      checkedNarrationFields,
    }
  }

  const senderNamePaths = [
    "paymentSourceInformation.accountName",
    "paymentSourceInformation.accountHolderName",
    "paymentSourceInformation.originatorAccountName",
    "sourceAccountInformation.accountName",
    "sourceAccountInformation.accountHolderName",
    "accountDetails.accountName",
    "accountPayments.accountName",
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
      checkedNarrations: Array.from(new Set(checkedNarrations)).slice(0, 12),
      checkedNarrationFields,
    }
  }

  return {
    name: "Anonymous",
    source: "anonymous",
    checkedNarrations: Array.from(new Set(checkedNarrations)).slice(0, 12),
    checkedNarrationFields,
  }
}

function sanitizeUser(user) {
  if (!user) return null

  const nameParts = getUserNameParts(user)
  const displayName = buildFullNameFromParts(nameParts, user.name || user.email || "Creator")

  return {
    id: user._id,
    name: displayName,
    firstName: nameParts.firstName,
    middleName: nameParts.middleName,
    lastName: nameParts.lastName,
    email: user.email,
    overlaySlug: getOverlaySlug(user),
    role: user.role || "creator",
    status: user.status || "active",
    profileImage: user.profileImage || "",
    identity: sanitizeIdentity(user.identity, nameParts),
    payoutProfile: sanitizePayoutProfile(user.payoutProfile),
    virtualAccount: user.virtualAccount || null,
    createdAt: user.createdAt,
  }
}

function escapeRegex(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
}

function parsePositiveInteger(value, fallback) {
  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}

function getPaginationMeta({ page, limit, total }) {
  const totalPages = Math.max(1, Math.ceil(total / limit))

  return {
    page,
    limit,
    total,
    totalPages,
    hasPrevious: page > 1,
    hasNext: page < totalPages,
  }
}

const MONNIFY_AUTHORIZATION_MESSAGE_REGEX = /authorization|otp|expired|expire/i
const MONNIFY_RETRYABLE_MESSAGE_REGEX = /authorization|otp|expired|expire|provider|monnify/i

function hasPendingMonnifyAuthorizationMessage(value) {
  return MONNIFY_AUTHORIZATION_MESSAGE_REGEX.test(String(value || ""))
}

function hasRetryableMonnifyMessage(value) {
  return MONNIFY_RETRYABLE_MESSAGE_REGEX.test(String(value || ""))
}

function isExpiredMonnifyTransferStatus(status) {
  return /EXPIRED|EXPIRY|OTP_EXPIRED|AUTHORIZATION_EXPIRED|TIMED_OUT|TIMEOUT/i.test(
    String(status || ""),
  )
}

function payoutRequiresMonnifyAuthorization(payout) {
  return payout?.status === "pending" && hasPendingMonnifyAuthorizationMessage(payout.providerMessage)
}

function canRetryMonnifyPayout(payout) {
  if (!payout || !["approved", "not_required"].includes(payout.reviewStatus || "not_required")) {
    return false
  }

  if (payout.status === "pending") {
    return payoutRequiresMonnifyAuthorization(payout)
  }

  if (["failed", "cancelled"].includes(payout.status)) {
    return payout.reviewStatus === "approved" && hasRetryableMonnifyMessage(payout.providerMessage)
  }

  return false
}

function sanitizePortalPayout(payout) {
  if (!payout) return null

  const creator =
    payout.creatorId && typeof payout.creatorId === "object" && (payout.creatorId._id || payout.creatorId.email)
      ? sanitizeUser(payout.creatorId)
      : undefined

  return {
    id: payout._id,
    amount: payout.amount,
    status: payout.status,
    reviewStatus: payout.reviewStatus,
    reviewReason: payout.reviewReason || "",
    reviewedBy: payout.reviewedBy || "",
    reviewedAt: payout.reviewedAt || "",
    rejectionReason: payout.rejectionReason || "",
    bankName: payout.bankName,
    bankCode: payout.bankCode,
    accountName: payout.accountName,
    accountNumber: payout.accountNumber,
    transferReference: payout.transferReference,
    providerReference: payout.providerReference || "",
    providerMessage: payout.providerMessage || "",
    requiresAuthorization: payoutRequiresMonnifyAuthorization(payout),
    canRetryTransfer: canRetryMonnifyPayout(payout),
    previousTransferReferences: payout.previousTransferReferences || [],
    retryCount: payout.retryCount || 0,
    createdAt: payout.createdAt,
    completedAt: payout.completedAt || "",
    creator,
  }
}

function sanitizePortalDonation(donation, options = {}) {
  if (!donation) return null

  const creator =
    donation.creatorId && typeof donation.creatorId === "object" && (donation.creatorId._id || donation.creatorId.email)
      ? sanitizeUser(donation.creatorId)
      : undefined
  const creatorId = creator?.id || donation.creatorId?.toString?.() || donation.creatorId || ""
  const item = {
    id: donation._id,
    creatorId,
    creatorEmail: donation.creatorEmail || creator?.email || "",
    creator,
    sender: donation.sender || "Anonymous",
    senderNameSource: donation.senderNameSource || "",
    amount: donation.amount || 0,
    platformFee:
      typeof donation.platformFee === "number"
        ? donation.platformFee
        : calculateRevenueSplit(donation.amount).platformFee,
    creatorShare:
      typeof donation.creatorShare === "number"
        ? donation.creatorShare
        : calculateRevenueSplit(donation.amount).creatorShare,
    eventType: donation.eventType || "",
    paymentStatus: donation.paymentStatus || "",
    paymentMethod: donation.paymentMethod || "",
    currency: donation.currency || "NGN",
    paidOn: donation.paidOn || "",
    sourceAccountName: donation.sourceAccountName || "",
    sourceAccountNumber: donation.sourceAccountNumber || "",
    sourceBankName: donation.sourceBankName || "",
    sourceBankCode: donation.sourceBankCode || "",
    sourceSessionId: donation.sourceSessionId || "",
    destinationAccountNumber: donation.destinationAccountNumber || "",
    destinationBankName: donation.destinationBankName || "",
    destinationBankCode: donation.destinationBankCode || "",
    monnifyTransactionReference: donation.monnifyTransactionReference || "",
    monnifyPaymentReference: donation.monnifyPaymentReference || "",
    date: donation.date,
  }

  if (options.includeProviderPayload) {
    item.providerPayload = donation.providerPayload || null
  }

  return item
}

function sanitizePortalComplianceInflow(inflow, options = {}) {
  if (!inflow) return null

  const linkedCreator =
    inflow.linkedCreatorId &&
    typeof inflow.linkedCreatorId === "object" &&
    (inflow.linkedCreatorId._id || inflow.linkedCreatorId.email)
      ? sanitizeUser(inflow.linkedCreatorId)
      : undefined

  const linkedDonation =
    inflow.linkedDonationId &&
    typeof inflow.linkedDonationId === "object" &&
    inflow.linkedDonationId._id
      ? sanitizePortalDonation(inflow.linkedDonationId)
      : undefined

  const item = {
    id: inflow._id,
    sourceAccountName: inflow.sourceAccountName || "",
    sourceAccountNumber: inflow.sourceAccountNumber || "",
    sourceBankName: inflow.sourceBankName || "",
    sourceBankCode: inflow.sourceBankCode || "",
    sourceSessionId: inflow.sourceSessionId || "",
    amount: inflow.amount || 0,
    currency: inflow.currency || "NGN",
    paymentStatus: inflow.paymentStatus || "",
    paymentMethod: inflow.paymentMethod || "",
    eventType: inflow.eventType || "",
    paidOn: inflow.paidOn || "",
    destinationAccountNumber: inflow.destinationAccountNumber || "",
    destinationBankName: inflow.destinationBankName || "",
    destinationBankCode: inflow.destinationBankCode || "",
    reservedAccountReference: inflow.reservedAccountReference || "",
    monnifyTransactionReference: inflow.monnifyTransactionReference || "",
    monnifyPaymentReference: inflow.monnifyPaymentReference || "",
    status: inflow.status || "held",
    validationReason: inflow.validationReason || "",
    adminNotes: inflow.adminNotes || "",
    linkedCreatorId:
      linkedCreator?.id || inflow.linkedCreatorId?.toString?.() || inflow.linkedCreatorId || "",
    linkedCreatorEmail: inflow.linkedCreatorEmail || linkedCreator?.email || "",
    linkedCreator,
    linkedDonationId:
      linkedDonation?.id || inflow.linkedDonationId?.toString?.() || inflow.linkedDonationId || "",
    linkedDonation,
    resolvedBy: inflow.resolvedBy || "",
    resolvedAt: inflow.resolvedAt || "",
    date: inflow.date,
    updatedAt: inflow.updatedAt || "",
  }

  if (options.includeRawPayload) {
    item.rawMonnifyPayload = inflow.rawMonnifyPayload || null
  }

  return item
}

async function buildPortalSettlementFilter(query = {}) {
  const filter = {}
  const status = String(query.status || "").trim()
  const reviewStatus = String(query.reviewStatus || "").trim()
  const from = String(query.from || "").trim()
  const to = String(query.to || "").trim()
  const search = String(query.search || "").trim()

  if (status && status !== "all") {
    filter.status = status
  }

  if (reviewStatus && reviewStatus !== "all") {
    filter.reviewStatus = reviewStatus
  }

  if (from || to) {
    filter.createdAt = {}

    if (from) {
      const fromDate = new Date(from)
      if (!Number.isNaN(fromDate.getTime())) {
        filter.createdAt.$gte = fromDate
      }
    }

    if (to) {
      const toDate = new Date(to)
      if (!Number.isNaN(toDate.getTime())) {
        toDate.setHours(23, 59, 59, 999)
        filter.createdAt.$lte = toDate
      }
    }

    if (!Object.keys(filter.createdAt).length) {
      delete filter.createdAt
    }
  }

  const searchFilter = await buildPortalSettlementSearchFilter(search)

  if (searchFilter) {
    filter.$and = [...(filter.$and || []), searchFilter]
  }

  return filter
}

async function buildPortalSettlementSearchFilter(search) {
  const cleanSearch = String(search || "").trim()

  if (!cleanSearch) {
    return null
  }

  const regex = new RegExp(escapeRegex(cleanSearch), "i")
  const matchingUsers = await User.find({
    $or: [
      { email: regex },
      { name: regex },
      { firstName: regex },
      { lastName: regex },
      { "identity.firstName": regex },
      { "identity.lastName": regex },
    ],
  })
    .select("_id")
    .limit(1000)

  const searchClauses = [
    { transferReference: regex },
    { providerReference: regex },
    { bankName: regex },
    { accountName: regex },
    { accountNumber: regex },
  ]

  if (matchingUsers.length) {
    searchClauses.push({ creatorId: { $in: matchingUsers.map((user) => user._id) } })
  }

  return { $or: searchClauses }
}

async function buildPortalSettlementQueueFilter(query = {}) {
  const baseFilter = {
    $or: [
      { status: "awaiting_review", reviewStatus: "awaiting_review" },
      {
        status: "pending",
        reviewStatus: { $in: ["approved", "not_required"] },
        providerMessage: MONNIFY_AUTHORIZATION_MESSAGE_REGEX,
      },
      {
        status: { $in: ["failed", "cancelled"] },
        reviewStatus: "approved",
        providerMessage: MONNIFY_RETRYABLE_MESSAGE_REGEX,
      },
    ],
  }
  const searchFilter = await buildPortalSettlementSearchFilter(query.search)

  if (!searchFilter) {
    return baseFilter
  }

  return { $and: [baseFilter, searchFilter] }
}

async function buildPortalDonationFilter(query = {}) {
  const filter = {}
  const search = String(query.search || "").trim()
  const creatorId = String(query.creatorId || "").trim()
  const from = String(query.from || "").trim()
  const to = String(query.to || "").trim()

  if (creatorId && mongoose.Types.ObjectId.isValid(creatorId)) {
    filter.creatorId = creatorId
  }

  if (from || to) {
    filter.date = {}

    if (from) {
      const fromDate = new Date(from)
      if (!Number.isNaN(fromDate.getTime())) {
        filter.date.$gte = fromDate
      }
    }

    if (to) {
      const toDate = new Date(to)
      if (!Number.isNaN(toDate.getTime())) {
        toDate.setHours(23, 59, 59, 999)
        filter.date.$lte = toDate
      }
    }

    if (!Object.keys(filter.date).length) {
      delete filter.date
    }
  }

  const searchFilter = await buildPortalDonationSearchFilter(search)

  if (searchFilter) {
    filter.$and = [...(filter.$and || []), searchFilter]
  }

  return filter
}

async function buildPortalDonationSearchFilter(search) {
  const cleanSearch = String(search || "").trim()

  if (!cleanSearch) {
    return null
  }

  const regex = new RegExp(escapeRegex(cleanSearch), "i")
  const matchingUsers = await User.find({
    $or: [
      { email: regex },
      { name: regex },
      { firstName: regex },
      { lastName: regex },
      { "identity.firstName": regex },
      { "identity.lastName": regex },
      { "virtualAccount.accountNumber": regex },
      { "virtualAccount.accountName": regex },
      { "virtualAccount.bankName": regex },
    ],
  })
    .select("_id")
    .limit(1000)

  const searchClauses = [
    { creatorEmail: regex },
    { sender: regex },
    { sourceAccountName: regex },
    { sourceAccountNumber: regex },
    { sourceBankName: regex },
    { sourceBankCode: regex },
    { sourceSessionId: regex },
    { destinationAccountNumber: regex },
    { destinationBankName: regex },
    { destinationBankCode: regex },
    { monnifyTransactionReference: regex },
    { monnifyPaymentReference: regex },
    { paymentMethod: regex },
    { eventType: regex },
  ]

  if (mongoose.Types.ObjectId.isValid(cleanSearch)) {
    searchClauses.push({ _id: cleanSearch })
  }

  if (matchingUsers.length) {
    searchClauses.push({ creatorId: { $in: matchingUsers.map((user) => user._id) } })
  }

  return { $or: searchClauses }
}

async function buildPortalComplianceInflowFilter(query = {}) {
  const filter = {}
  const status = String(query.status || "").trim()
  const from = String(query.from || "").trim()
  const to = String(query.to || "").trim()
  const search = String(query.search || "").trim()

  if (status && status !== "all") {
    filter.status = status
  }

  if (from || to) {
    filter.date = {}

    if (from) {
      const fromDate = new Date(from)
      if (!Number.isNaN(fromDate.getTime())) {
        filter.date.$gte = fromDate
      }
    }

    if (to) {
      const toDate = new Date(to)
      if (!Number.isNaN(toDate.getTime())) {
        toDate.setHours(23, 59, 59, 999)
        filter.date.$lte = toDate
      }
    }

    if (!Object.keys(filter.date).length) {
      delete filter.date
    }
  }

  const searchFilter = await buildPortalComplianceInflowSearchFilter(search)

  if (searchFilter) {
    filter.$and = [...(filter.$and || []), searchFilter]
  }

  return filter
}

async function buildPortalComplianceInflowSearchFilter(search) {
  const cleanSearch = String(search || "").trim()

  if (!cleanSearch) {
    return null
  }

  const regex = new RegExp(escapeRegex(cleanSearch), "i")
  const matchingUsers = await User.find({
    $or: [
      { email: regex },
      { name: regex },
      { firstName: regex },
      { lastName: regex },
      { "identity.firstName": regex },
      { "identity.lastName": regex },
      { "virtualAccount.accountNumber": regex },
      { "virtualAccount.accountReference": regex },
      { "virtualAccount.reservationReference": regex },
    ],
  })
    .select("_id")
    .limit(1000)

  const searchClauses = [
    { sourceAccountName: regex },
    { sourceAccountNumber: regex },
    { sourceBankName: regex },
    { sourceBankCode: regex },
    { sourceSessionId: regex },
    { destinationAccountNumber: regex },
    { destinationBankName: regex },
    { destinationBankCode: regex },
    { reservedAccountReference: regex },
    { monnifyTransactionReference: regex },
    { monnifyPaymentReference: regex },
    { paymentStatus: regex },
    { eventType: regex },
    { validationReason: regex },
    { adminNotes: regex },
    { linkedCreatorEmail: regex },
  ]

  if (mongoose.Types.ObjectId.isValid(cleanSearch)) {
    searchClauses.push({ _id: cleanSearch })
    searchClauses.push({ linkedCreatorId: cleanSearch })
    searchClauses.push({ linkedDonationId: cleanSearch })
  }

  if (matchingUsers.length) {
    searchClauses.push({ linkedCreatorId: { $in: matchingUsers.map((user) => user._id) } })
  }

  return { $or: searchClauses }
}

function csvCell(value) {
  const safeValue = String(value ?? "")
  return `"${safeValue.replace(/"/g, '""')}"`
}

function settlementReportFilename() {
  const stamp = new Date().toISOString().slice(0, 10)
  return `streamtip-settlements-${stamp}.csv`
}

function donationReportFilename() {
  const stamp = new Date().toISOString().slice(0, 10)
  return `streamtip-gifts-${stamp}.csv`
}

function complianceInflowReportFilename() {
  const stamp = new Date().toISOString().slice(0, 10)
  return `streamtip-compliance-inflows-${stamp}.csv`
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

  const monnifyCode =
    error.response?.data?.responseCode ||
    error.response?.data?.responseBody?.responseCode ||
    error.response?.data?.responseBody?.code
  const monnifyMessage =
    error.response?.data?.responseMessage ||
    error.response?.data?.responseBody?.message ||
    error.response?.data?.message

  if (
    String(monnifyCode || "").toUpperCase() === "D06" ||
    String(monnifyMessage || "").toLowerCase().includes("unauthorized request")
  ) {
    return "Monnify rejected this withdrawal because the backend server IP is not authorized for live disbursement. Please contact support."
  }

  if (typeof monnifyMessage === "string" && monnifyMessage.trim()) {
    return monnifyMessage.trim()
  }

  return error.message || fallbackMessage
}

function getAxiosErrorDetails(error) {
  if (!(error instanceof AxiosError)) {
    return {}
  }

  return {
    httpStatus: error.response?.status || "",
    responseCode:
      error.response?.data?.responseCode ||
      error.response?.data?.responseBody?.responseCode ||
      error.response?.data?.responseBody?.code ||
      "",
    responseMessage:
      error.response?.data?.responseMessage ||
      error.response?.data?.responseBody?.message ||
      error.response?.data?.message ||
      error.message ||
      "",
  }
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

function isReservedAccountAlreadyMissingError(error) {
  if (!(error instanceof AxiosError)) {
    return false
  }

  const status = Number(error.response?.status || 0)
  const responseCode = String(
    error.response?.data?.responseCode ||
      error.response?.data?.responseBody?.responseCode ||
      error.response?.data?.responseBody?.code ||
      "",
  )
    .trim()
    .toUpperCase()
  const message = getAxiosErrorMessage(error, "").toLowerCase()

  return (
    status === 404 ||
    message.includes("cannot find reserved account") ||
    message.includes("reserved account not found") ||
    (responseCode === "99" && message.includes("not found"))
  )
}

function isReservedAccountAlreadyMissingResponse(response) {
  const responseCode = String(response?.responseCode || response?.responseBody?.responseCode || "")
    .trim()
    .toUpperCase()
  const message = String(response?.responseMessage || response?.message || response?.responseBody?.message || "")
    .toLowerCase()
    .trim()

  return (
    response?.requestSuccessful === false &&
    (message.includes("cannot find reserved account") ||
      message.includes("reserved account not found") ||
      (responseCode === "99" && message.includes("not found")))
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

async function getMonnifyTransactionStatus(transactionReference) {
  if (!isMonnifyConfigured()) {
    throw new Error("Monnify is not configured yet.")
  }

  const normalizedReference = String(transactionReference || "").trim()

  if (!normalizedReference) {
    throw new Error("A Monnify transaction reference is required.")
  }

  const accessToken = await getMonnifyAccessToken()
  const response = await axios.get(
    `${MONNIFY_BASE_URL}/api/v2/transactions/${encodeURIComponent(normalizedReference)}`,
    {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        Accept: "application/json",
      },
    },
  )

  return response.data?.responseBody || response.data || {}
}

function assignIfPresent(target, field, value) {
  const normalizedValue = value instanceof Date ? value : compactString(value)

  if (normalizedValue) {
    target[field] = normalizedValue
  }
}

async function syncDonationFromMonnifyTransaction(donation) {
  if (!donation) {
    throw new Error("Donation not found.")
  }

  const transactionReference = String(
    donation.monnifyTransactionReference || donation.monnifyPaymentReference || "",
  ).trim()

  if (!transactionReference) {
    throw new Error("This donation does not have a Monnify reference to refresh.")
  }

  const providerPayload = await getMonnifyTransactionStatus(transactionReference)
  const sourceDetails = getDonationSourceDetails(providerPayload, providerPayload)
  const destinationDetails = getDonationDestinationDetails(providerPayload, providerPayload)
  const paidOn = parseProviderDate(providerPayload.paidOn || providerPayload.paidAt)
  const split = calculateRevenueSplit(
    firstValidMoneyAmount(
      providerPayload.amountPaid,
      providerPayload.amount,
      providerPayload.settlementAmount,
      providerPayload.totalPayable,
      donation.amount,
    ),
  )

  assignIfPresent(donation, "sourceAccountName", sourceDetails.sourceAccountName)
  assignIfPresent(donation, "sourceAccountNumber", sourceDetails.sourceAccountNumber)
  assignIfPresent(donation, "sourceBankName", sourceDetails.sourceBankName)
  assignIfPresent(donation, "sourceBankCode", sourceDetails.sourceBankCode)
  assignIfPresent(donation, "sourceSessionId", sourceDetails.sourceSessionId)
  assignIfPresent(donation, "destinationAccountNumber", destinationDetails.destinationAccountNumber)
  assignIfPresent(donation, "destinationBankName", destinationDetails.destinationBankName)
  assignIfPresent(donation, "destinationBankCode", destinationDetails.destinationBankCode)
  assignIfPresent(donation, "paymentMethod", providerPayload.paymentMethod)
  assignIfPresent(donation, "paymentStatus", providerPayload.paymentStatus)
  assignIfPresent(donation, "currency", providerPayload.currency)
  assignIfPresent(donation, "paidOn", paidOn)
  assignIfPresent(donation, "monnifyTransactionReference", providerPayload.transactionReference)
  assignIfPresent(donation, "monnifyPaymentReference", providerPayload.paymentReference)

  if (split.gross > 0) {
    donation.amount = split.gross
    donation.platformFee = split.platformFee
    donation.creatorShare = split.creatorShare
  }

  donation.providerPayload = providerPayload
  await donation.save()

  return donation
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

async function deallocateMonnifyReservedAccount(accountReference) {
  if (!isMonnifyConfigured()) {
    throw new Error(
      "Monnify is not configured yet. Add MONNIFY_API_KEY, MONNIFY_SECRET_KEY, MONNIFY_CONTRACT_CODE, and MONNIFY_BASE_URL to Backend/.env.",
    )
  }

  const normalizedReference = String(accountReference || "").trim()

  if (!normalizedReference) {
    throw new Error("No Monnify account reference is saved for this virtual account.")
  }

  const accessToken = await getMonnifyAccessToken()
  const response = await axios.delete(
    `${MONNIFY_BASE_URL}/api/v1/bank-transfer/reserved-accounts/reference/${encodeURIComponent(
      normalizedReference,
    )}`,
    {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        Accept: "application/json",
        "Content-Type": "application/json",
      },
    },
  )

  return response.data || {}
}

async function removeVirtualAccountForUser(user) {
  const previousVirtualAccount = user.virtualAccount || null
  const accountReference = String(previousVirtualAccount?.accountReference || "").trim()
  const accountStatus = String(previousVirtualAccount?.status || "").toLowerCase()
  const remoteDeallocation = {
    attempted: false,
    succeeded: false,
    alreadyMissing: false,
    responseMessage: "",
  }

  if (!previousVirtualAccount?.accountNumber) {
    return {
      user,
      previousVirtualAccount: null,
      remoteDeallocation,
    }
  }

  if (accountReference && accountStatus !== "pending" && !accountReference.startsWith("PENDING-")) {
    remoteDeallocation.attempted = true

    try {
      const monnifyResponse = await deallocateMonnifyReservedAccount(accountReference)
      remoteDeallocation.responseMessage = String(
        monnifyResponse.responseMessage || monnifyResponse.message || "Reserved account deallocated.",
      ).trim()

      if (isReservedAccountAlreadyMissingResponse(monnifyResponse)) {
        remoteDeallocation.alreadyMissing = true
      } else if (monnifyResponse.requestSuccessful === false) {
        throw new Error(
          remoteDeallocation.responseMessage || "Could not deallocate the Monnify reserved account.",
        )
      } else {
        remoteDeallocation.succeeded = true
      }
    } catch (error) {
      if (!isReservedAccountAlreadyMissingError(error)) {
        const message =
          error instanceof AxiosError
            ? getAxiosErrorMessage(error, "Could not deallocate the Monnify reserved account.")
            : error instanceof Error
              ? error.message
              : "Could not deallocate the Monnify reserved account."

        throw new Error(message)
      }

      remoteDeallocation.alreadyMissing = true
      remoteDeallocation.responseMessage = getAxiosErrorMessage(
        error,
        "Reserved account was already missing in Monnify.",
      )
    }
  }

  const updatedUser = await User.findByIdAndUpdate(
    user._id,
    { $unset: { virtualAccount: "" } },
    { new: true },
  )

  return {
    user: updatedUser || user,
    previousVirtualAccount,
    remoteDeallocation,
  }
}

function createTransferReference(prefix = "STIP-PAYOUT") {
  return `${prefix}-${Date.now()}-${Math.round(Math.random() * 1_000_000)}`
}

function formatNotificationCurrency(value) {
  return `NGN ${Number(value || 0).toLocaleString("en-NG")}`
}

function formatNotificationDate(value = new Date()) {
  const date = value instanceof Date ? value : new Date(value)

  if (Number.isNaN(date.getTime())) {
    return new Date().toISOString()
  }

  return new Intl.DateTimeFormat("en-NG", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "Africa/Lagos",
  }).format(date)
}

function maskAccountNumber(value) {
  const accountNumber = String(value || "")
  return accountNumber ? `**** ${accountNumber.slice(-4)}` : "No account"
}

function getWithdrawalReviewReasonLabel(reasons = []) {
  const labels = {
    portal_review_required: "Admin portal review required",
    first_withdrawal: "First withdrawal",
  }
  const cleanReasons = Array.isArray(reasons)
    ? reasons.filter(Boolean)
    : String(reasons || "").split(",").filter(Boolean)

  if (!cleanReasons.length) {
    return "Standard withdrawal"
  }

  return cleanReasons.map((reason) => labels[reason] || String(reason).replace(/_/g, " ")).join(", ")
}

function getWithdrawalNotificationStatus({ payout, reviewReasons, providerError }) {
  if (providerError) {
    return "Monnify initiation failed"
  }

  if (reviewReasons?.length || payout.reviewStatus === "awaiting_review") {
    return "Awaiting admin review"
  }

  if (payout.status === "completed") {
    return "Sent successfully"
  }

  if (payout.status === "pending") {
    return "Queued with Monnify"
  }

  return payout.status || "Requested"
}

function buildWithdrawalTelegramMessage({
  payout,
  creator,
  availableCreatorBalance,
  reviewReasons = [],
  providerError = "",
}) {
  const portalUrl = PORTAL_URL.replace(/\/$/, "")
  const status = getWithdrawalNotificationStatus({ payout, reviewReasons, providerError })
  const lines = [
    "StreamTip withdrawal request",
    "",
    `Streamer: ${creator.name || "Creator"}`,
    `Email: ${creator.email || "Not available"}`,
    `Amount: ${formatNotificationCurrency(payout.amount)}`,
    `Available balance before request: ${formatNotificationCurrency(availableCreatorBalance)}`,
    `Status: ${status}`,
    `Review reason: ${getWithdrawalReviewReasonLabel(reviewReasons)}`,
    `Bank: ${payout.bankName || "Not available"}`,
    `Account: ${payout.accountName || "No account name"} - ${maskAccountNumber(payout.accountNumber)}`,
    `Transfer ref: ${payout.transferReference || "Not available"}`,
    `Requested: ${formatNotificationDate(payout.createdAt || new Date())}`,
  ]

  if (providerError) {
    lines.push(`Provider error: ${providerError}`)
  }

  if (portalUrl) {
    lines.push("", `Portal: ${portalUrl}`)
  }

  return lines.join("\n")
}

async function sendTelegramMessage(text) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_WITHDRAWAL_CHAT_ID) {
    return { skipped: true, reason: "Telegram withdrawal notification env is not configured." }
  }

  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`

  await axios.post(
    url,
    {
      chat_id: TELEGRAM_WITHDRAWAL_CHAT_ID,
      text,
      disable_web_page_preview: true,
    },
    { timeout: 10_000 },
  )

  return { skipped: false }
}

async function notifyWithdrawalRequestOnTelegram(options) {
  try {
    const result = await sendTelegramMessage(buildWithdrawalTelegramMessage(options))

    if (result.skipped) {
      console.warn("telegram.withdrawal_notification_skipped", result.reason)
    }
  } catch (error) {
    const message =
      error instanceof AxiosError
        ? getAxiosErrorMessage(error, "Telegram withdrawal notification failed.")
        : error instanceof Error
          ? error.message
          : "Telegram withdrawal notification failed."

    console.error("telegram.withdrawal_notification_failed", message)
  }
}

function normalizeMonnifyTransferStatus(status) {
  const normalized = String(status || "").toUpperCase()

  if (["SUCCESS", "SUCCESSFUL", "COMPLETED", "SUCCESSFUL_DISBURSEMENT"].includes(normalized)) {
    return "completed"
  }

  if (isExpiredMonnifyTransferStatus(normalized)) {
    return "pending"
  }

  if (["CANCELLED", "CANCELED"].includes(normalized)) {
    return "cancelled"
  }

  if (
    [
      "FAILED",
      "FAILED_DISBURSEMENT",
      "REJECTED",
      "REVERSED",
      "REVERSED_DISBURSEMENT",
    ].includes(normalized)
  ) {
    return "failed"
  }

  return "pending"
}

function extractMonnifyDisbursementReference(eventData = {}, data = {}) {
  return String(
    eventData.reference ||
      eventData.transferReference ||
      eventData.transactionReference ||
      eventData.transactionRef ||
      eventData.paymentReference ||
      data.reference ||
      data.transferReference ||
      data.transactionReference ||
      data.paymentReference ||
      "",
  ).trim()
}

function extractMonnifyDisbursementStatus(eventData = {}, data = {}) {
  return String(
    eventData.status ||
      eventData.paymentStatus ||
      eventData.transactionStatus ||
      eventData.transferStatus ||
      data.status ||
      data.paymentStatus ||
      data.transactionStatus ||
      data.eventType ||
      "",
  ).trim()
}

async function getMonnifyDisbursementSummary(reference) {
  if (!isMonnifyConfigured()) {
    throw new Error("Monnify disbursement is not configured yet.")
  }

  const accessToken = await getMonnifyAccessToken()
  const response = await axios.get(`${MONNIFY_BASE_URL}/api/v2/disbursements/single/summary`, {
    headers: {
      Authorization: `Bearer ${accessToken}`,
      Accept: "application/json",
    },
    params: { reference },
  })

  return response.data?.responseBody || {}
}

async function updatePayoutFromProviderStatus(payout, providerPayload = {}, actorType = "system") {
  const providerStatus = extractMonnifyDisbursementStatus(providerPayload, providerPayload)
  const normalizedStatus = normalizeMonnifyTransferStatus(providerStatus)

  payout.status = normalizedStatus
  payout.providerReference =
    String(
      providerPayload.transactionReference ||
        providerPayload.paymentReference ||
        providerPayload.reference ||
        payout.providerReference ||
        payout.transferReference,
    ).trim() || payout.transferReference
  payout.providerMessage = String(
    providerPayload.message ||
      providerPayload.responseMessage ||
      providerPayload.narration ||
      providerStatus ||
      payout.providerMessage ||
      "",
  ).trim()

  if (["completed", "failed", "cancelled"].includes(normalizedStatus)) {
    payout.completedAt = payout.completedAt || new Date()
  } else {
    payout.completedAt = undefined
  }

  await payout.save()

  io.to(getCreatorRoom(payout.creatorId)).emit("newPayout", payout)

  await createAuditLog({
    actorType,
    eventType: "payout.status_synced",
    message: `Payout ${payout.transferReference} synced as ${normalizedStatus}.`,
    metadata: {
      payoutId: payout._id.toString(),
      transferReference: payout.transferReference,
      providerStatus,
      providerReference: payout.providerReference || "",
    },
  })

  return payout
}

async function reconcilePendingPayouts() {
  if (!isDatabaseConnected() || !isMonnifyConfigured()) {
    return { checked: 0, updated: 0 }
  }

  const pendingPayouts = await Payout.find({
    transferReference: { $exists: true, $ne: "" },
    $or: [
      { status: "pending" },
      { status: "failed", reviewStatus: "approved" },
    ],
  })
    .sort({ createdAt: 1 })
    .limit(50)

  let updated = 0

  for (const payout of pendingPayouts) {
    try {
      const summary = await getMonnifyDisbursementSummary(payout.transferReference)
      const beforeStatus = payout.status
      await updatePayoutFromProviderStatus(payout, summary, "system")
      if (payout.status !== beforeStatus) {
        updated += 1
      }
    } catch (error) {
      await createAuditLog({
        actorType: "system",
        eventType: "payout.reconciliation.failed",
        message: `Could not reconcile payout ${payout.transferReference}.`,
        metadata: {
          payoutId: payout._id.toString(),
          transferReference: payout.transferReference,
          error: error instanceof Error ? error.message : String(error),
        },
      })
    }
  }

  return { checked: pendingPayouts.length, updated }
}

async function failPayoutAndReleaseBalance({
  payout,
  actorId,
  reason = "Payout failed and creator balance was released.",
  eventType = "portal.payout.failed_released",
}) {
  payout.status = "failed"
  payout.reviewStatus = payout.reviewStatus === "awaiting_review" ? "rejected" : payout.reviewStatus
  payout.reviewedBy = actorId
  payout.reviewedAt = new Date()
  payout.rejectionReason = reason
  payout.providerMessage = reason
  payout.completedAt = new Date()
  await payout.save()

  io.to(getCreatorRoom(payout.creatorId)).emit("newPayout", payout)

  await createAuditLog({
    actorType: "admin",
    actorId,
    eventType,
    message: `Admin marked payout ${payout.transferReference} as failed and released the creator balance.`,
    metadata: {
      payoutId: payout._id.toString(),
      creatorId: payout.creatorId.toString(),
      amount: payout.amount,
      transferReference: payout.transferReference,
      reason,
    },
  })

  return payout
}

async function retryPayoutWithNewTransferReference({ payout, creator, actorId }) {
  if (!canRetryMonnifyPayout(payout)) {
    throw new Error("This payout is not eligible for a Monnify retry.")
  }

  if (["failed", "cancelled"].includes(payout.status)) {
    const totals = await getRevenueTotals({ creatorId: payout.creatorId })
    const availableBalance = Number(totals.creatorAvailableBalance) || 0

    if (availableBalance < (Number(payout.amount) || 0)) {
      throw new Error(
        "This payout balance has already been released and is no longer available for retry.",
      )
    }
  }

  const previousTransferReference = String(payout.transferReference || "").trim()
  const previousProviderReference = String(payout.providerReference || "").trim()
  const nextTransferReference = createTransferReference()
  const previousReferences = Array.isArray(payout.previousTransferReferences)
    ? payout.previousTransferReferences
    : []

  payout.status = "pending"
  payout.transferReference = nextTransferReference
  payout.providerReference = nextTransferReference
  payout.providerMessage = previousTransferReference
    ? `Retrying Monnify transfer after expired authorization. Previous transfer ref: ${previousTransferReference}.`
    : "Retrying Monnify transfer after expired authorization."
  payout.rejectionReason = ""
  payout.completedAt = undefined
  payout.previousTransferReferences = Array.from(
    new Set([...previousReferences, previousTransferReference].filter(Boolean)),
  ).slice(-10)
  payout.retryCount = (Number(payout.retryCount) || 0) + 1
  await payout.save()

  const sentPayout = await sendPayoutToMonnify(payout, creator, "admin", actorId)

  await createAuditLog({
    actorType: "admin",
    actorId,
    eventType: "portal.settlement.retried",
    message: `Admin retried payout ${previousTransferReference || payout._id} with a new Monnify reference.`,
    metadata: {
      payoutId: payout._id.toString(),
      creatorId: creator._id.toString(),
      amount: payout.amount,
      previousTransferReference,
      previousProviderReference,
      transferReference: sentPayout.transferReference,
      providerReference: sentPayout.providerReference || "",
      retryCount: sentPayout.retryCount || 0,
    },
  })

  return sentPayout
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

  const disbursement = response.data?.responseBody || {}
  const responseMessage = String(response.data?.responseMessage || disbursement.message || "").trim()
  const responseCode = String(response.data?.responseCode || disbursement.responseCode || "").trim()

  if (response.data?.requestSuccessful === false) {
    throw new Error(responseMessage || "Monnify rejected the disbursement request.")
  }

  const disbursementReference = String(disbursement.reference || reference || "").trim()

  if (!disbursementReference) {
    throw new Error("Monnify did not return a disbursement reference.")
  }

  return {
    ...disbursement,
    reference: disbursementReference,
    responseMessage,
    responseCode,
  }
}

async function authorizeMonnifySingleDisbursement({ reference, authorizationCode }) {
  if (!isMonnifyConfigured()) {
    throw new Error("Monnify disbursement is not configured yet.")
  }

  const cleanReference = String(reference || "").trim()
  const cleanAuthorizationCode = String(authorizationCode || "").trim()

  if (!cleanReference || !cleanAuthorizationCode) {
    throw new Error("Transfer reference and Monnify OTP are required.")
  }

  const accessToken = await getMonnifyAccessToken()
  const response = await axios.post(
    `${MONNIFY_BASE_URL}/api/v2/disbursements/single/validate-otp`,
    {
      reference: cleanReference,
      authorizationCode: cleanAuthorizationCode,
    },
    {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": "application/json",
      },
    },
  )
  const authorization = response.data?.responseBody || {}
  const responseMessage = String(response.data?.responseMessage || authorization.message || "").trim()
  const responseCode = String(response.data?.responseCode || authorization.responseCode || "").trim()

  if (response.data?.requestSuccessful === false) {
    throw new Error(responseMessage || "Monnify rejected the authorization code.")
  }

  return {
    ...authorization,
    reference: authorization.reference || cleanReference,
    responseMessage,
    responseCode,
  }
}

async function resendMonnifySingleDisbursementOtp(reference) {
  if (!isMonnifyConfigured()) {
    throw new Error("Monnify disbursement is not configured yet.")
  }

  const cleanReference = String(reference || "").trim()

  if (!cleanReference) {
    throw new Error("Transfer reference is required.")
  }

  const accessToken = await getMonnifyAccessToken()
  const response = await axios.post(
    `${MONNIFY_BASE_URL}/api/v2/disbursements/single/resend-otp`,
    { reference: cleanReference },
    {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": "application/json",
      },
    },
  )
  const responseBody = response.data?.responseBody || {}
  const responseMessage = String(
    response.data?.responseMessage || responseBody.message || "Monnify OTP resent.",
  ).trim()

  if (response.data?.requestSuccessful === false) {
    throw new Error(responseMessage || "Could not resend Monnify OTP.")
  }

  return {
    ...responseBody,
    responseMessage,
  }
}

async function getCurrentUser() {
  return User.findOne().sort({ createdAt: -1 })
}

async function getRevenueTotals({ creatorId } = {}) {
  const donationQuery = creatorId ? { creatorId } : {}
  const payoutQuery = {
    status: { $nin: ["failed", "rejected", "cancelled"] },
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

async function hasSuccessfulWithdrawal(creatorId) {
  const completedPayout = await Payout.findOne({
    creatorId,
    status: "completed",
  }).sort({ createdAt: 1 })

  return Boolean(completedPayout)
}

function getLockedPayoutProfile(user) {
  const profile = user?.payoutProfile || {}

  if (
    !profile.locked ||
    profile.status !== "verified" ||
    !profile.bankCode ||
    !profile.accountNumber ||
    !profile.accountName
  ) {
    throw new Error("Add and verify your locked payout bank account before requesting withdrawals.")
  }

  return profile
}

async function buildVerifiedPayoutProfile({
  user,
  bankCode,
  bankName,
  accountNumber,
  firstName,
  middleName,
  lastName,
}) {
  const cleanBankCode = String(bankCode || "").trim()
  const cleanBankName = String(bankName || "").trim()
  const cleanAccountNumber = String(accountNumber || "").replace(/\D/g, "").slice(0, 10)
  const fallbackParts = getUserNameParts(user)
  const legal = validateLegalNameParts({
    firstName: resolvePayoutNamePart(firstName, fallbackParts.firstName),
    middleName: resolvePayoutNamePart(middleName, fallbackParts.middleName),
    lastName: resolvePayoutNamePart(lastName, fallbackParts.lastName),
  })

  if (!cleanBankCode || cleanAccountNumber.length !== 10) {
    throw new Error("Bank and 10-digit account number are required.")
  }

  const resolvedAccount = await resolveBankAccountName({
    bankCode: cleanBankCode,
    accountNumber: cleanAccountNumber,
  })

  validatePayoutAccountNameMatch({
    ...legal,
    accountName: resolvedAccount.accountName,
  })

  return {
    bankName: cleanBankName || resolvedAccount.bankName || "",
    bankCode: resolvedAccount.bankCode || cleanBankCode,
    accountNumber: resolvedAccount.accountNumber || cleanAccountNumber,
    accountName: resolvedAccount.accountName,
    firstName: legal.first,
    middleName: legal.middle,
    lastName: legal.last,
    status: "verified",
    locked: true,
    verifiedAt: new Date(),
    lockedAt: new Date(),
    changeRequiresSupport: true,
    verificationProvider: "monnify",
  }
}

async function sendPayoutToMonnify(payout, creator, actorType = "system", actorId = "") {
  let transferResponse

  try {
    transferResponse = await initiateMonnifyDisbursement({
      amount: Number(payout.amount) || 0,
      bankCode: payout.bankCode,
      accountNumber: payout.accountNumber,
      accountName: payout.accountName,
      narration: `StreamTip creator payout for ${creator.name}`,
      reference: payout.transferReference || createTransferReference(),
    })
  } catch (error) {
    const message = getAxiosErrorMessage(error, "Failed to initiate payout with Monnify.")
    payout.status = "failed"
    payout.providerMessage = message
    payout.completedAt = new Date()
    await payout.save()

    await createAuditLog({
      actorType,
      actorId,
      eventType: "payout.failed",
      message: `Payout failed for ${payout.bankName}.`,
      metadata: {
        ...getAxiosErrorDetails(error),
        payoutId: payout._id.toString(),
        amount: payout.amount,
        creatorId: creator._id.toString(),
        transferReference: payout.transferReference,
        error: message,
      },
    })

    io.to(getCreatorRoom(creator._id)).emit("newPayout", payout)
    throw new Error(message)
  }

  const providerStatus =
    transferResponse.status ||
    transferResponse.paymentStatus ||
    transferResponse.transactionStatus ||
    ""
  const payoutStatus = normalizeMonnifyTransferStatus(providerStatus)

  payout.status = payoutStatus
  payout.providerReference =
    String(
      transferResponse.transactionReference ||
        transferResponse.reference ||
        transferResponse.paymentReference ||
        payout.transferReference,
    ).trim() || payout.transferReference
  payout.providerMessage = String(
    [
      providerStatus,
      transferResponse.message || transferResponse.responseMessage || "",
    ]
      .filter(Boolean)
      .join(" - "),
  ).trim()
  payout.completedAt = payoutStatus === "completed" ? new Date() : undefined
  await payout.save()

  io.to(getCreatorRoom(creator._id)).emit("newPayout", payout)

  await createAuditLog({
    actorType,
    actorId,
    eventType: "payout.sent_to_monnify",
    message: `Payout sent to Monnify for ${payout.bankName}.`,
    metadata: {
      payoutId: payout._id.toString(),
      amount: payout.amount,
      creatorId: creator._id.toString(),
      transferReference: payout.transferReference,
      providerReference: payout.providerReference || "",
      providerStatus: payout.status,
    },
  })

  return payout
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
    databaseName: mongoose.connection.name || "",
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
    const {
      email,
      password,
      bvn = "",
      nin = "",
      dateOfBirth = "",
      firstName = "",
      middleName = "",
      lastName = "",
      acceptIdentityPolicy = false,
      bankName = "",
      bankCode = "",
      accountNumber = "",
    } = req.body

    if (!email || !password) {
      return res.status(400).json({ error: "Email and password are required." })
    }

    const normalizedEmail = String(email).toLowerCase().trim()
    const rawPassword = String(password)
    const trimmedBvn = String(bvn || "").trim()
    const trimmedNin = String(nin || "").trim()
    const trimmedDateOfBirth = String(dateOfBirth || "").trim()
    const legal = validateLegalNameParts({ firstName, middleName, lastName })
    const trimmedName = buildFullNameFromParts({
      firstName: legal.first,
      middleName: legal.middle,
      lastName: legal.last,
    })

    if (rawPassword.length < 8) {
      return res.status(400).json({ error: "Password must be at least 8 characters long." })
    }

    if (!acceptIdentityPolicy) {
      return res.status(400).json({ error: "Accept the identity policy before creating an account." })
    }

    if (!/^\d{11}$/.test(trimmedBvn)) {
      return res.status(400).json({ error: "BVN must be 11 digits." })
    }

    if (!/^\d{11}$/.test(trimmedNin)) {
      return res.status(400).json({ error: "NIN must be 11 digits." })
    }

    if (!/^\d{4}-\d{2}-\d{2}$/.test(trimmedDateOfBirth)) {
      return res.status(400).json({ error: "Date of birth is required." })
    }

    let user = await User.findOne({ email: normalizedEmail })

    if (user) {
      return res.status(409).json({ error: "An account with that email already exists." })
    }

    const payoutProfile = await buildVerifiedPayoutProfile({
      user: {
        identity: {
          firstName: legal.first,
          middleName: legal.middle,
          lastName: legal.last,
        },
      },
      bankCode,
      bankName,
      accountNumber,
    })

    user = await User.create({
      name: trimmedName,
      firstName: legal.first,
      middleName: legal.middle,
      lastName: legal.last,
      email: normalizedEmail,
      passwordHash: hashPassword(rawPassword),
      sessionToken: generateSessionToken(),
      role: "creator",
      status: "active",
      identity: {
        bvn: trimmedBvn,
        nin: trimmedNin,
        dateOfBirth: trimmedDateOfBirth,
        firstName: legal.first,
        middleName: legal.middle,
        lastName: legal.last,
        policyAcceptedAt: new Date(),
        signupCompletedAt: new Date(),
      },
      payoutProfile,
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
    const eventType = String(data.eventType || eventData.eventType || "monnify.webhook")
    const disbursementReference = extractMonnifyDisbursementReference(eventData, data)

    if (/disbursement|transfer/i.test(eventType) && disbursementReference) {
      const payout = await Payout.findOne({
        $or: [
          { transferReference: disbursementReference },
          { providerReference: disbursementReference },
        ],
      })

      if (!payout) {
        await createAuditLog({
          actorType: "system",
          eventType: "webhook.monnify.unmatched_payout",
          message: "Monnify disbursement webhook could not be matched to a payout.",
          metadata: {
            eventType,
            disbursementReference,
          },
        })

        return res.sendStatus(200)
      }

      await updatePayoutFromProviderStatus(payout, { ...eventData, eventType }, "system")
      return res.sendStatus(200)
    }

    const creator = await findCreatorForMonnifyEvent(eventData)
    const paymentStatus = String(
      eventData.paymentStatus || eventData.status || data.paymentStatus || "PENDING",
    ).toUpperCase()
    const { transactionReference, paymentReference } = getMonnifyPaymentReferences(eventData, data)
    const destinationDetails = getDonationDestinationDetails(eventData, data)
    const destinationAccountNumber = destinationDetails.destinationAccountNumber
    const destinationBankName = destinationDetails.destinationBankName
    const sourceDetails = getDonationSourceDetails(eventData, data)
    const paymentMethod = compactString(eventData.paymentMethod || data.paymentMethod)
    const currency = compactString(eventData.currency || data.currency || "NGN")
    const paidOn = parseProviderDate(eventData.paidOn || eventData.paidAt || data.paidOn || data.paidAt)
    const grossAmount = firstValidMoneyAmount(
      eventData.amountPaid,
      eventData.amount,
      eventData.settlementAmount,
      eventData.totalPayable,
      data.amount,
      data.amountPaid,
    )
    const split = calculateRevenueSplit(grossAmount)

    if (paymentStatus && paymentStatus !== "PAID") {
      await upsertComplianceInflowFromMonnify({
        eventType,
        eventData,
        data,
        creator,
        status: normalizeComplianceInflowStatus({
          paymentStatus,
          creator,
          destinationAccountNumber,
          amount: grossAmount,
        }),
      })

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
        await upsertComplianceInflowFromMonnify({
          eventType,
          eventData,
          data,
          existingDonation: existingByTransactionRef,
          status: "resolved",
        })
        return res.sendStatus(200)
      }
    }

    if (paymentReference) {
      const existingByPaymentRef = await Donation.findOne({
        monnifyPaymentReference: paymentReference,
      })

      if (existingByPaymentRef) {
        await upsertComplianceInflowFromMonnify({
          eventType,
          eventData,
          data,
          existingDonation: existingByPaymentRef,
          status: "resolved",
        })
        return res.sendStatus(200)
      }
    }

    if (split.gross <= 0) {
      await upsertComplianceInflowFromMonnify({
        eventType,
        eventData,
        data,
        creator,
        status: "unmatched",
      })

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
      await upsertComplianceInflowFromMonnify({
        eventType,
        eventData,
        data,
        status: "unmatched",
      })

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
    const resolvedSenderDisplay =
      senderDisplay.name === "Anonymous" && sourceDetails.sourceAccountName
        ? {
            ...senderDisplay,
            name: getFirstNameOnly(sourceDetails.sourceAccountName),
            source: "source_account_first_name",
          }
        : senderDisplay
    const senderNameResolutionMetadata = {
      sender: resolvedSenderDisplay.name,
      senderNameSource: resolvedSenderDisplay.source,
      checkedNarrations: resolvedSenderDisplay.checkedNarrations || [],
      checkedNarrationFields: (resolvedSenderDisplay.checkedNarrationFields || []).map((entry) => ({
        ...entry,
        rejectedAsSystem: isSystemNarration(entry.value, []),
      })),
      creatorId: creator._id.toString(),
      transactionReference,
      paymentReference,
    }
    console.info("donation.sender_name_resolution", senderNameResolutionMetadata)

    const donation = await Donation.create({
      creatorId: creator._id,
      creatorEmail: creator.email,
      sender: resolvedSenderDisplay.name,
      senderNameSource: resolvedSenderDisplay.source,
      amount: split.gross,
      platformFee: split.platformFee,
      creatorShare: split.creatorShare,
      eventType,
      paymentStatus,
      paymentMethod: paymentMethod || undefined,
      currency: currency || undefined,
      paidOn,
      ...sourceDetails,
      destinationAccountNumber: destinationAccountNumber || undefined,
      destinationBankName: destinationBankName || undefined,
      destinationBankCode: destinationDetails.destinationBankCode || undefined,
      monnifyTransactionReference: transactionReference || undefined,
      monnifyPaymentReference: paymentReference || undefined,
      providerPayload: data,
    })

    await upsertComplianceInflowFromMonnify({
      eventType,
      eventData,
      data,
      creator,
      donation,
      status: "resolved",
    })

    io.to(getCreatorRoom(creator._id)).emit("newDonation", donation)

    await createAuditLog({
      actorType: "system",
      eventType: "donation.sender_name_resolution",
      message: `Donation sender display resolved as ${resolvedSenderDisplay.source}.`,
      metadata: senderNameResolutionMetadata,
    })

    await createAuditLog({
      actorType: "system",
      eventType: "donation.received",
      message: `Donation received from ${resolvedSenderDisplay.name}.`,
      metadata: {
        sender: resolvedSenderDisplay.name,
        senderNameSource: resolvedSenderDisplay.source,
        checkedNarrations: resolvedSenderDisplay.checkedNarrations || [],
        checkedNarrationFields: resolvedSenderDisplay.checkedNarrationFields || [],
        amount: split.gross,
        platformFee: split.platformFee,
        creatorShare: split.creatorShare,
        creatorId: creator._id.toString(),
        paymentStatus,
        paymentMethod,
        currency,
        paidOn,
        sourceAccountName: sourceDetails.sourceAccountName,
        sourceAccountNumber: sourceDetails.sourceAccountNumber,
        sourceBankName: sourceDetails.sourceBankName,
        sourceBankCode: sourceDetails.sourceBankCode,
        sourceSessionId: sourceDetails.sourceSessionId,
        destinationAccountNumber: destinationDetails.destinationAccountNumber,
        destinationBankName: destinationDetails.destinationBankName,
        destinationBankCode: destinationDetails.destinationBankCode,
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

app.get("/uploads/gift-sounds/:soundId", async (req, res) => {
  try {
    if (!mongoose.Types.ObjectId.isValid(req.params.soundId)) {
      return res.status(404).send("Sound not found.")
    }

    const sound = await GiftSound.findById(req.params.soundId)

    if (!sound?.data?.length) {
      return res.status(404).send("Sound not found.")
    }

    res.setHeader("Content-Type", sound.contentType || "audio/mpeg")
    res.setHeader("Cache-Control", "public, max-age=31536000, immutable")
    return res.send(sound.data)
  } catch (error) {
    console.error(error)
    return res.status(500).send("Failed to load gift sound.")
  }
})

app.post(
  "/overlay-sound-upload",
  requireSessionUser,
  express.raw({ type: ["audio/*", "application/octet-stream"], limit: "3mb" }),
  async (req, res) => {
    try {
      const mimeType = String(req.headers["content-type"] || "").split(";")[0].trim()
      const originalFileName = String(req.headers["x-file-name"] || "Uploaded audio").slice(0, 80)
      const savedSound = await saveGiftSoundUpload({
        req,
        buffer: req.body,
        mimeType,
        originalFileName,
      })

      return res.status(201).json(savedSound)
    } catch (error) {
      console.error(error)
      return res.status(error.statusCode || 500).json({
        error: error instanceof Error ? error.message : "Failed to upload gift sound.",
      })
    }
  },
)

app.post("/overlay-sound-upload-json", requireSessionUser, async (req, res) => {
  try {
    const dataUrl = String(req.body?.dataUrl || "")
    const match = dataUrl.match(/^data:([^;]+);base64,(.+)$/)

    if (!match) {
      return res.status(400).json({ error: "Invalid audio upload payload." })
    }

    const savedSound = await saveGiftSoundUpload({
      req,
      buffer: Buffer.from(match[2], "base64"),
      mimeType: String(req.body?.mimeType || match[1]),
      originalFileName: String(req.body?.fileName || "Uploaded audio").slice(0, 80),
    })

    return res.status(201).json(savedSound)
  } catch (error) {
    console.error(error)
    return res.status(error.statusCode || 500).json({
      error: error instanceof Error ? error.message : "Failed to upload gift sound.",
    })
  }
})

app.put("/overlay-state", requireSessionUser, async (req, res) => {
  try {
    const currentState = getOverlayStateForUser(req.user)
    const nextCustomGifts = Array.isArray(req.body?.customGifts)
      ? await normalizeCustomGiftSoundUploads(req, req.body.customGifts)
      : currentState.customGifts
    const nextState = {
      settings: req.body?.settings ? req.body.settings : currentState.settings,
      customization: req.body?.customization ? req.body.customization : currentState.customization,
      customGifts: nextCustomGifts,
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

app.get("/public/banks", async (_req, res) => {
  try {
    const banks = await getSupportedBanks()
    res.json({ banks })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to load supported banks." })
  }
})

app.post("/public/bank-account-name-enquiry", async (req, res) => {
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
    const { amount } = req.body
    const payoutAmount = Number(amount) || 0
    const payoutProfile = getLockedPayoutProfile(req.user)

    const creatorTotals = await getRevenueTotals({ creatorId: req.user._id })
    const availableCreatorBalance = creatorTotals.creatorAvailableBalance
    const hasCompletedWithdrawal = await hasSuccessfulWithdrawal(req.user._id)

    if (!payoutAmount || payoutAmount <= 0) {
      return res.status(400).json({ error: "Enter a valid payout amount." })
    }

    if (payoutAmount < MIN_CREATOR_WITHDRAWAL) {
      return res.status(400).json({
        error: `Minimum withdrawal is NGN ${MIN_CREATOR_WITHDRAWAL.toLocaleString()}.`,
        minimumWithdrawal: MIN_CREATOR_WITHDRAWAL,
      })
    }

    if (payoutAmount > availableCreatorBalance) {
      return res.status(400).json({
        error: "Creators can only withdraw up to their 80% share of earnings.",
        availableCreatorBalance,
        creatorRevenue: creatorTotals.creatorRevenue,
        platformRevenue: creatorTotals.platformRevenue,
      })
    }

    const transferReference = createTransferReference()
    const requiresReviewReasons = ["portal_review_required"]

    if (!hasCompletedWithdrawal) {
      requiresReviewReasons.push("first_withdrawal")
    }

    const payout = await Payout.create({
      creatorId: req.user._id,
      amount: payoutAmount,
      bankName: payoutProfile.bankName,
      bankCode: payoutProfile.bankCode,
      accountNumber: payoutProfile.accountNumber,
      accountName: payoutProfile.accountName,
      status: "awaiting_review",
      reviewStatus: "awaiting_review",
      reviewReason: requiresReviewReasons.join(","),
      transferReference,
      providerReference: transferReference,
      providerMessage: "Awaiting StreamTip settlement review before Monnify transfer.",
      createdAt: new Date(),
    })

    io.to(getCreatorRoom(req.user._id)).emit("newPayout", payout)

    await createAuditLog({
      actorType: "system",
      eventType: "payout.awaiting_review",
      message: `Payout is awaiting review for ${payoutProfile.bankName}.`,
      metadata: {
        amount: payoutAmount,
        bankName: payoutProfile.bankName,
        bankCode: payoutProfile.bankCode,
        accountNumber: String(payoutProfile.accountNumber).slice(-4),
        accountName: payoutProfile.accountName,
        creatorId: req.user._id.toString(),
        transferReference,
        reviewReason: payout.reviewReason || "",
      },
    })

    void notifyWithdrawalRequestOnTelegram({
      payout,
      creator: req.user,
      availableCreatorBalance,
      reviewReasons: requiresReviewReasons,
    })

    res.json(payout)
  } catch (error) {
    console.error(error)
    res.status(400).json({
      error: error instanceof Error ? error.message : "Failed payout",
    })
  }
})

app.get("/donations", requireSessionUser, async (req, res) => {
  try {
    const donations = await Donation.find({ creatorId: req.user._id }).sort({ date: -1 })
    res.json(donations)
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to load donations." })
  }
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

    const nextEmail = String(req.body?.email || "").toLowerCase().trim()
    const nextProfileImage = String(req.body?.profileImage || "").trim()
    const currentPassword = String(req.body?.currentPassword || "")
    const newPassword = String(req.body?.newPassword || "")
    const nextBvn = typeof req.body?.bvn === "string" ? req.body.bvn.trim() : undefined
    const nextNin = typeof req.body?.nin === "string" ? req.body.nin.trim() : undefined
    const currentNameParts = getUserNameParts(user)
    const nextNameParts = validateLegalNameParts({
      firstName:
        typeof req.body?.firstName === "string" ? req.body.firstName : currentNameParts.firstName,
      middleName:
        typeof req.body?.middleName === "string" ? req.body.middleName : currentNameParts.middleName,
      lastName:
        typeof req.body?.lastName === "string" ? req.body.lastName : currentNameParts.lastName,
    })

    if (!nextEmail) {
      return res.status(400).json({ error: "Email is required." })
    }

    const existingUser = await User.findOne({
      email: nextEmail,
      _id: { $ne: user._id },
    })

    if (existingUser) {
      return res.status(409).json({ error: "That email is already in use." })
    }

    applyUserNameParts(user, {
      firstName: nextNameParts.first,
      middleName: nextNameParts.middle,
      lastName: nextNameParts.last,
    })
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
      user.virtualAccount.accountName = user.virtualAccount.accountName || `StreamTip/${user.name}`
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

app.get("/portal/compliance-inflows", requireAdminSession, async (req, res) => {
  try {
    const page = parsePositiveInteger(req.query.page, 1)
    const limit = Math.min(parsePositiveInteger(req.query.limit, 25), 100)
    const skip = (page - 1) * limit
    const filter = await buildPortalComplianceInflowFilter(req.query)

    const [inflows, total] = await Promise.all([
      ComplianceInflow.find(filter)
        .populate("linkedCreatorId")
        .populate("linkedDonationId")
        .sort({ date: -1 })
        .skip(skip)
        .limit(limit),
      ComplianceInflow.countDocuments(filter),
    ])

    res.json({
      inflows: inflows.map((inflow) => sanitizePortalComplianceInflow(inflow)),
      pagination: getPaginationMeta({ page, limit, total }),
      statuses: complianceInflowStatuses,
      search: String(req.query.search || "").trim(),
    })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to load compliance inflows." })
  }
})

app.get("/portal/compliance-inflows/report", requireAdminSession, async (req, res) => {
  try {
    const filter = await buildPortalComplianceInflowFilter(req.query)
    const headers = [
      "Compliance Inflow ID",
      "Date",
      "Paid At",
      "Status",
      "Validation Reason",
      "Admin Notes",
      "Linked Creator",
      "Linked Creator Email",
      "Linked Donation ID",
      "Amount",
      "Currency",
      "Payment Status",
      "Payment Method",
      "Source Account Name",
      "Source Account Number",
      "Source Bank",
      "Source Bank Code",
      "Source Session ID",
      "Destination Account Number",
      "Destination Bank",
      "Destination Bank Code",
      "Reserved Account Reference",
      "Monnify Transaction Reference",
      "Monnify Payment Reference",
      "Event Type",
      "Resolved At",
      "Updated At",
    ]

    res.setHeader("Content-Type", "text/csv; charset=utf-8")
    res.setHeader("Content-Disposition", `attachment; filename="${complianceInflowReportFilename()}"`)
    res.write(`${headers.map(csvCell).join(",")}\n`)

    const cursor = ComplianceInflow.find(filter)
      .populate("linkedCreatorId")
      .populate("linkedDonationId")
      .sort({ date: -1 })
      .cursor()

    for await (const inflow of cursor) {
      const item = sanitizePortalComplianceInflow(inflow)
      const row = [
        item.id,
        item.date ? new Date(item.date).toISOString() : "",
        item.paidOn ? new Date(item.paidOn).toISOString() : "",
        item.status || "",
        item.validationReason || "",
        item.adminNotes || "",
        item.linkedCreator?.name || "",
        item.linkedCreatorEmail || item.linkedCreator?.email || "",
        item.linkedDonationId || "",
        item.amount || 0,
        item.currency || "",
        item.paymentStatus || "",
        item.paymentMethod || "",
        item.sourceAccountName || "",
        item.sourceAccountNumber || "",
        item.sourceBankName || "",
        item.sourceBankCode || "",
        item.sourceSessionId || "",
        item.destinationAccountNumber || "",
        item.destinationBankName || "",
        item.destinationBankCode || "",
        item.reservedAccountReference || "",
        item.monnifyTransactionReference || "",
        item.monnifyPaymentReference || "",
        item.eventType || "",
        item.resolvedAt ? new Date(item.resolvedAt).toISOString() : "",
        item.updatedAt ? new Date(item.updatedAt).toISOString() : "",
      ]

      res.write(`${row.map(csvCell).join(",")}\n`)
    }

    res.end()
  } catch (error) {
    console.error(error)
    if (res.headersSent) {
      res.end()
      return
    }
    res.status(500).json({ error: "Failed to export compliance inflow report." })
  }
})

app.get("/portal/compliance-inflows/:id", requireAdminSession, async (req, res) => {
  try {
    if (!mongoose.Types.ObjectId.isValid(req.params.id)) {
      return res.status(400).json({ error: "Invalid compliance inflow ID." })
    }

    const inflow = await ComplianceInflow.findById(req.params.id)
      .populate("linkedCreatorId")
      .populate("linkedDonationId")

    if (!inflow) {
      return res.status(404).json({ error: "Compliance inflow not found." })
    }

    res.json({
      inflow: sanitizePortalComplianceInflow(inflow, { includeRawPayload: true }),
      statuses: complianceInflowStatuses,
    })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to load compliance inflow." })
  }
})

app.patch("/portal/compliance-inflows/:id", requireAdminSession, async (req, res) => {
  try {
    if (!mongoose.Types.ObjectId.isValid(req.params.id)) {
      return res.status(400).json({ error: "Invalid compliance inflow ID." })
    }

    const inflow = await ComplianceInflow.findById(req.params.id)

    if (!inflow) {
      return res.status(404).json({ error: "Compliance inflow not found." })
    }

    const nextStatus = String(req.body?.status || inflow.status || "").trim()
    const adminNotes = String(req.body?.adminNotes ?? inflow.adminNotes ?? "").trim()
    const linkedCreatorId = String(req.body?.linkedCreatorId || "").trim()

    if (nextStatus && !complianceInflowStatuses.includes(nextStatus)) {
      return res.status(400).json({ error: "Invalid compliance inflow status." })
    }

    if (linkedCreatorId) {
      if (!mongoose.Types.ObjectId.isValid(linkedCreatorId)) {
        return res.status(400).json({ error: "Invalid linked creator ID." })
      }

      const linkedCreator = await User.findById(linkedCreatorId)

      if (!linkedCreator) {
        return res.status(404).json({ error: "Linked creator not found." })
      }

      inflow.linkedCreatorId = linkedCreator._id
      inflow.linkedCreatorEmail = linkedCreator.email
    } else if (Object.prototype.hasOwnProperty.call(req.body || {}, "linkedCreatorId")) {
      inflow.linkedCreatorId = undefined
      inflow.linkedCreatorEmail = ""
    }

    if (nextStatus) {
      inflow.status = nextStatus
    }

    inflow.adminNotes = adminNotes
    inflow.updatedAt = new Date()

    if (inflow.status === "resolved" && !inflow.resolvedAt) {
      inflow.resolvedAt = new Date()
      inflow.resolvedBy = req.adminSession._id.toString()
    }

    await inflow.save()

    await createAuditLog({
      actorType: "admin",
      actorId: req.adminSession._id.toString(),
      eventType: "portal.compliance_inflow.updated",
      message: `Admin updated compliance inflow ${inflow._id}.`,
      metadata: {
        inflowId: inflow._id.toString(),
        status: inflow.status,
        linkedCreatorId: inflow.linkedCreatorId?.toString?.() || "",
        monnifyTransactionReference: inflow.monnifyTransactionReference || "",
        monnifyPaymentReference: inflow.monnifyPaymentReference || "",
      },
    })

    const populated = await ComplianceInflow.findById(inflow._id)
      .populate("linkedCreatorId")
      .populate("linkedDonationId")

    res.json({
      inflow: sanitizePortalComplianceInflow(populated || inflow, { includeRawPayload: true }),
    })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to update compliance inflow." })
  }
})

app.get("/portal/donations", requireAdminSession, async (req, res) => {
  try {
    const page = parsePositiveInteger(req.query.page, 1)
    const limit = Math.min(parsePositiveInteger(req.query.limit, 25), 100)
    const skip = (page - 1) * limit
    const filter = await buildPortalDonationFilter(req.query)

    const [donations, total] = await Promise.all([
      Donation.find(filter)
        .populate("creatorId")
        .sort({ date: -1 })
        .skip(skip)
        .limit(limit),
      Donation.countDocuments(filter),
    ])

    res.json({
      donations: donations.map((donation) => sanitizePortalDonation(donation)),
      pagination: getPaginationMeta({ page, limit, total }),
      search: String(req.query.search || "").trim(),
    })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to load donation transactions." })
  }
})

app.get("/portal/donations/report", requireAdminSession, async (req, res) => {
  try {
    const filter = await buildPortalDonationFilter(req.query)
    const headers = [
      "Donation ID",
      "Received At",
      "Provider Paid At",
      "Creator Name",
      "Creator Email",
      "Amount",
      "Platform Fee",
      "Creator Share",
      "Payment Status",
      "Payment Method",
      "Currency",
      "Source Account Name",
      "Source Account Number",
      "Source Bank",
      "Source Bank Code",
      "Source Session ID",
      "Destination Account Number",
      "Destination Bank",
      "Destination Bank Code",
      "Monnify Transaction Reference",
      "Monnify Payment Reference",
      "Sender Display",
      "Sender Name Source",
      "Event Type",
    ]

    res.setHeader("Content-Type", "text/csv; charset=utf-8")
    res.setHeader("Content-Disposition", `attachment; filename="${donationReportFilename()}"`)
    res.write(`${headers.map(csvCell).join(",")}\n`)

    const cursor = Donation.find(filter).populate("creatorId").sort({ date: -1 }).cursor()

    for await (const donation of cursor) {
      const item = sanitizePortalDonation(donation)
      const row = [
        item.id,
        item.date ? new Date(item.date).toISOString() : "",
        item.paidOn ? new Date(item.paidOn).toISOString() : "",
        item.creator?.name || "",
        item.creatorEmail || item.creator?.email || "",
        item.amount || 0,
        item.platformFee || 0,
        item.creatorShare || 0,
        item.paymentStatus || "",
        item.paymentMethod || "",
        item.currency || "",
        item.sourceAccountName || "",
        item.sourceAccountNumber || "",
        item.sourceBankName || "",
        item.sourceBankCode || "",
        item.sourceSessionId || "",
        item.destinationAccountNumber || "",
        item.destinationBankName || "",
        item.destinationBankCode || "",
        item.monnifyTransactionReference || "",
        item.monnifyPaymentReference || "",
        item.sender || "",
        item.senderNameSource || "",
        item.eventType || "",
      ]

      res.write(`${row.map(csvCell).join(",")}\n`)
    }

    res.end()
  } catch (error) {
    console.error(error)
    if (res.headersSent) {
      res.end()
      return
    }
    res.status(500).json({ error: "Failed to export donation report." })
  }
})

app.get("/portal/donations/:id", requireAdminSession, async (req, res) => {
  try {
    if (!mongoose.Types.ObjectId.isValid(req.params.id)) {
      return res.status(400).json({ error: "Invalid donation ID." })
    }

    const donation = await Donation.findById(req.params.id).populate("creatorId")

    if (!donation) {
      return res.status(404).json({ error: "Donation not found." })
    }

    res.json({
      donation: sanitizePortalDonation(donation, { includeProviderPayload: true }),
    })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to load donation transaction." })
  }
})

app.post("/portal/donations/:id/sync-provider", requireAdminSession, async (req, res) => {
  try {
    if (!mongoose.Types.ObjectId.isValid(req.params.id)) {
      return res.status(400).json({ error: "Invalid donation ID." })
    }

    const donation = await Donation.findById(req.params.id)

    if (!donation) {
      return res.status(404).json({ error: "Donation not found." })
    }

    const syncedDonation = await syncDonationFromMonnifyTransaction(donation)
    const populatedDonation = await Donation.findById(syncedDonation._id).populate("creatorId")

    await createAuditLog({
      actorType: "admin",
      actorId: req.adminSession._id.toString(),
      eventType: "portal.donation.provider_synced",
      message: `Admin refreshed donation ${syncedDonation._id} from Monnify.`,
      metadata: {
        donationId: syncedDonation._id.toString(),
        transactionReference: syncedDonation.monnifyTransactionReference || "",
        sourceAccountName: syncedDonation.sourceAccountName || "",
        sourceAccountNumber: syncedDonation.sourceAccountNumber || "",
        sourceSessionId: syncedDonation.sourceSessionId || "",
      },
    })

    res.json({
      donation: sanitizePortalDonation(populatedDonation || syncedDonation, { includeProviderPayload: true }),
    })
  } catch (error) {
    console.error(error)
    res.status(502).json({
      error: error instanceof Error ? error.message : "Failed to refresh donation from Monnify.",
    })
  }
})

app.get("/portal/settlements", requireAdminSession, async (req, res) => {
  try {
    const page = parsePositiveInteger(req.query.page, 1)
    const limit = Math.min(parsePositiveInteger(req.query.limit, 20), 100)
    const skip = (page - 1) * limit
    const filter = await buildPortalSettlementQueueFilter(req.query)

    const [payouts, total] = await Promise.all([
      Payout.find(filter)
        .populate("creatorId")
        .sort({ createdAt: 1 })
        .skip(skip)
        .limit(limit),
      Payout.countDocuments(filter),
    ])

    res.json({
      payouts: payouts.map(sanitizePortalPayout),
      pagination: getPaginationMeta({ page, limit, total }),
    })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to load settlement queue." })
  }
})

app.get("/portal/settlements/history", requireAdminSession, async (req, res) => {
  try {
    const page = parsePositiveInteger(req.query.page, 1)
    const limit = Math.min(parsePositiveInteger(req.query.limit, 20), 100)
    const skip = (page - 1) * limit
    const filter = await buildPortalSettlementFilter(req.query)

    const [payouts, total] = await Promise.all([
      Payout.find(filter)
        .populate("creatorId")
        .sort({ createdAt: -1 })
        .skip(skip)
        .limit(limit),
      Payout.countDocuments(filter),
    ])

    res.json({
      payouts: payouts.map(sanitizePortalPayout),
      pagination: getPaginationMeta({ page, limit, total }),
    })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to load settlement history." })
  }
})

app.get("/portal/settlements/report", requireAdminSession, async (req, res) => {
  try {
    const filter = await buildPortalSettlementFilter(req.query)
    const headers = [
      "Payout ID",
      "Creator First Name",
      "Creator Last Name",
      "Creator Email",
      "Amount",
      "Status",
      "Review Status",
      "Review Reason",
      "Rejection Reason",
      "Bank",
      "Account Name",
      "Account Number",
      "Transfer Reference",
      "Provider Reference",
      "Provider Message",
      "Requested At",
      "Reviewed At",
      "Completed At",
    ]

    res.setHeader("Content-Type", "text/csv; charset=utf-8")
    res.setHeader("Content-Disposition", `attachment; filename="${settlementReportFilename()}"`)
    res.write(`${headers.map(csvCell).join(",")}\n`)

    const cursor = Payout.find(filter).populate("creatorId").sort({ createdAt: -1 }).cursor()

    for await (const payout of cursor) {
      const item = sanitizePortalPayout(payout)
      const creator = item.creator || {}
      const row = [
        item.id,
        creator.firstName || "",
        creator.lastName || "",
        creator.email || "",
        item.amount || 0,
        item.status || "",
        item.reviewStatus || "",
        item.reviewReason || "",
        item.rejectionReason || "",
        item.bankName || "",
        item.accountName || "",
        item.accountNumber || "",
        item.transferReference || "",
        item.providerReference || "",
        item.providerMessage || "",
        item.createdAt ? new Date(item.createdAt).toISOString() : "",
        item.reviewedAt ? new Date(item.reviewedAt).toISOString() : "",
        item.completedAt ? new Date(item.completedAt).toISOString() : "",
      ]

      res.write(`${row.map(csvCell).join(",")}\n`)
    }

    res.end()
  } catch (error) {
    console.error(error)
    if (res.headersSent) {
      res.end()
      return
    }
    res.status(500).json({ error: "Failed to export settlement report." })
  }
})

app.post("/portal/settlements/:id/approve", requireAdminSession, async (req, res) => {
  try {
    const payout = await Payout.findById(req.params.id)

    if (!payout) {
      return res.status(404).json({ error: "Payout not found." })
    }

    if (payout.status !== "awaiting_review" || payout.reviewStatus !== "awaiting_review") {
      return res.status(409).json({ error: "This payout is not awaiting review." })
    }

    const creator = await User.findById(payout.creatorId)

    if (!creator) {
      return res.status(404).json({ error: "Creator not found." })
    }

    payout.reviewStatus = "approved"
    payout.reviewedBy = req.adminSession._id.toString()
    payout.reviewedAt = new Date()
    payout.providerMessage = "Approved in StreamTip portal. Sending to Monnify."
    await payout.save()

    const sentPayout = await sendPayoutToMonnify(
      payout,
      creator,
      "admin",
      req.adminSession._id.toString(),
    )

    await createAuditLog({
      actorType: "admin",
      actorId: req.adminSession._id.toString(),
      eventType: "portal.settlement.approved",
      message: `Admin approved payout ${payout.transferReference}.`,
      metadata: {
        payoutId: payout._id.toString(),
        creatorId: creator._id.toString(),
        amount: payout.amount,
      },
    })

    res.json({ payout: sentPayout, creator: sanitizeUser(creator) })
  } catch (error) {
    console.error(error)
    res.status(502).json({
      error: error instanceof Error ? error.message : "Could not approve and send payout.",
    })
  }
})

app.post("/portal/settlements/:id/authorize", requireAdminSession, async (req, res) => {
  try {
    const payout = await Payout.findById(req.params.id)

    if (!payout) {
      return res.status(404).json({ error: "Payout not found." })
    }

    if (!payoutRequiresMonnifyAuthorization(payout)) {
      return res.status(409).json({ error: "This payout is not pending Monnify authorization." })
    }

    const authorizationCode = String(req.body?.authorizationCode || "").trim()

    if (!authorizationCode) {
      return res.status(400).json({ error: "Enter the Monnify OTP for this transfer." })
    }

    const authorization = await authorizeMonnifySingleDisbursement({
      reference: payout.transferReference,
      authorizationCode,
    })
    const updatedPayout = await updatePayoutFromProviderStatus(payout, authorization, "admin")

    await createAuditLog({
      actorType: "admin",
      actorId: req.adminSession._id.toString(),
      eventType: "portal.settlement.authorized",
      message: `Admin authorized payout ${payout.transferReference} with Monnify OTP.`,
      metadata: {
        payoutId: payout._id.toString(),
        transferReference: payout.transferReference,
        providerStatus: authorization.status || authorization.paymentStatus || "",
      },
    })

    res.json({ payout: updatedPayout })
  } catch (error) {
    console.error(error)
    res.status(502).json({
      error: error instanceof Error ? error.message : "Could not authorize Monnify payout.",
    })
  }
})

app.post("/portal/settlements/:id/resend-otp", requireAdminSession, async (req, res) => {
  try {
    const payout = await Payout.findById(req.params.id)

    if (!payout) {
      return res.status(404).json({ error: "Payout not found." })
    }

    if (!payoutRequiresMonnifyAuthorization(payout)) {
      return res.status(409).json({ error: "This payout is not pending Monnify authorization." })
    }

    const result = await resendMonnifySingleDisbursementOtp(payout.transferReference)
    payout.providerMessage = result.responseMessage || "Monnify OTP resent."
    await payout.save()

    await createAuditLog({
      actorType: "admin",
      actorId: req.adminSession._id.toString(),
      eventType: "portal.settlement.otp_resent",
      message: `Admin requested a new Monnify OTP for payout ${payout.transferReference}.`,
      metadata: {
        payoutId: payout._id.toString(),
        transferReference: payout.transferReference,
      },
    })

    res.json({ payout, message: payout.providerMessage })
  } catch (error) {
    console.error(error)
    res.status(502).json({
      error: error instanceof Error ? error.message : "Could not resend Monnify OTP.",
    })
  }
})

app.post("/portal/settlements/:id/retry", requireAdminSession, async (req, res) => {
  try {
    const payout = await Payout.findById(req.params.id)

    if (!payout) {
      return res.status(404).json({ error: "Payout not found." })
    }

    if (!canRetryMonnifyPayout(payout)) {
      return res.status(409).json({
        error: "This payout cannot be retried. Only approved Monnify authorization failures can be retried.",
      })
    }

    const creator = await User.findById(payout.creatorId)

    if (!creator) {
      return res.status(404).json({ error: "Creator not found." })
    }

    const retriedPayout = await retryPayoutWithNewTransferReference({
      payout,
      creator,
      actorId: req.adminSession._id.toString(),
    })

    res.json({ payout: retriedPayout, creator: sanitizeUser(creator) })
  } catch (error) {
    console.error(error)
    res.status(502).json({
      error: error instanceof Error ? error.message : "Could not retry Monnify payout.",
    })
  }
})

app.post("/portal/settlements/:id/reject", requireAdminSession, async (req, res) => {
  try {
    const payout = await Payout.findById(req.params.id)

    if (!payout) {
      return res.status(404).json({ error: "Payout not found." })
    }

    if (payout.status !== "awaiting_review" || payout.reviewStatus !== "awaiting_review") {
      return res.status(409).json({ error: "This payout is not awaiting review." })
    }

    const reason = String(req.body?.reason || "").trim()

    payout.status = "rejected"
    payout.reviewStatus = "rejected"
    payout.reviewedBy = req.adminSession._id.toString()
    payout.reviewedAt = new Date()
    payout.rejectionReason = reason || "Rejected by settlement reviewer."
    payout.providerMessage = payout.rejectionReason
    payout.completedAt = new Date()
    await payout.save()

    io.to(getCreatorRoom(payout.creatorId)).emit("newPayout", payout)

    await createAuditLog({
      actorType: "admin",
      actorId: req.adminSession._id.toString(),
      eventType: "portal.settlement.rejected",
      message: `Admin rejected payout ${payout.transferReference}.`,
      metadata: {
        payoutId: payout._id.toString(),
        creatorId: payout.creatorId.toString(),
        amount: payout.amount,
        reason: payout.rejectionReason,
      },
    })

    res.json({ payout })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Could not reject payout." })
  }
})

app.get("/portal/users", requireAdminSession, async (req, res) => {
  try {
    const page = parsePositiveInteger(req.query.page, 1)
    const limit = Math.min(parsePositiveInteger(req.query.limit, 25), 100)
    const skip = (page - 1) * limit
    const search = String(req.query.search || "").trim()
    const filter = {}

    if (search) {
      filter.email = { $regex: escapeRegex(search), $options: "i" }
    }

    const [users, total] = await Promise.all([
      User.find(filter).sort({ createdAt: -1 }).skip(skip).limit(limit),
      User.countDocuments(filter),
    ])

    res.json({
      users: users.map(sanitizeUser),
      pagination: getPaginationMeta({ page, limit, total }),
      search,
    })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to load portal users." })
  }
})

app.get("/portal/users/:id", requireAdminSession, async (req, res) => {
  try {
    if (!mongoose.Types.ObjectId.isValid(req.params.id)) {
      return res.status(400).json({ error: "Invalid user ID." })
    }

    const user = await User.findById(req.params.id)

    if (!user) {
      return res.status(404).json({ error: "User not found." })
    }

    const [payouts, changeRequests, donations] = await Promise.all([
      Payout.find({ creatorId: user._id }).sort({ createdAt: -1 }).limit(25),
      PayoutProfileChangeRequest.find({ creatorId: user._id }).sort({ createdAt: -1 }).limit(10),
      Donation.find({ creatorId: user._id }).sort({ date: -1 }).limit(50),
    ])

    res.json({
      user: sanitizeUser(user),
      payouts: payouts.map(sanitizePortalPayout),
      changeRequests: changeRequests.map(sanitizePayoutProfileChangeRequest),
      donations: donations.map(sanitizePortalDonation),
    })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to load portal user details." })
  }
})

app.patch("/portal/users/:id", requireAdminSession, async (req, res) => {
  try {
    const user = await User.findById(req.params.id)

    if (!user) {
      return res.status(404).json({ error: "User not found." })
    }

    const reason = String(req.body?.reason || "").trim()

    if (!reason) {
      return res.status(400).json({ error: "A super admin edit reason is required." })
    }

    if (typeof req.body?.email === "string" && req.body.email.trim()) {
      user.email = req.body.email.toLowerCase().trim()
    }

    if (["active", "suspended", "banned"].includes(String(req.body?.status))) {
      user.status = String(req.body.status)
    }

    const currentNameParts = getUserNameParts(user)
    const identityBody = req.body?.identity && typeof req.body.identity === "object" ? req.body.identity : {}
    const legal = validateLegalNameParts({
      firstName:
        typeof req.body?.firstName === "string"
          ? req.body.firstName
          : typeof identityBody.firstName === "string"
            ? identityBody.firstName
            : currentNameParts.firstName,
      middleName:
        typeof req.body?.middleName === "string"
          ? req.body.middleName
          : typeof identityBody.middleName === "string"
            ? identityBody.middleName
            : currentNameParts.middleName,
      lastName:
        typeof req.body?.lastName === "string"
          ? req.body.lastName
          : typeof identityBody.lastName === "string"
            ? identityBody.lastName
            : currentNameParts.lastName,
    })
    applyUserNameParts(user, {
      firstName: legal.first,
      middleName: legal.middle,
      lastName: legal.last,
    })

    if (typeof identityBody.dateOfBirth === "string") {
      user.identity = user.identity || {}
      user.identity.dateOfBirth = identityBody.dateOfBirth.trim()
    }

    await user.save()

    await createAuditLog({
      actorType: "admin",
      actorId: req.adminSession._id.toString(),
      eventType: "portal.user.super_admin_updated",
      message: `Super admin updated ${user.email}.`,
      metadata: {
        userId: user._id.toString(),
        reason,
        changedFields: Object.keys(req.body || {}).filter((key) => key !== "reason"),
      },
    })

    res.json({ user: sanitizeUser(user) })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to update portal user." })
  }
})

app.get("/portal/payout-profile-change-requests", requireAdminSession, async (_req, res) => {
  try {
    const requests = await PayoutProfileChangeRequest.find()
      .populate("creatorId")
      .sort({ createdAt: -1 })
      .limit(100)

    res.json({ requests: requests.map(sanitizePayoutProfileChangeRequest) })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to load payout profile change requests." })
  }
})

app.post("/portal/payout-profile-change-requests/:id/approve", requireAdminSession, async (req, res) => {
  try {
    const request = await PayoutProfileChangeRequest.findById(req.params.id)

    if (!request) {
      return res.status(404).json({ error: "Change request not found." })
    }

    if (request.status !== "awaiting_review") {
      return res.status(409).json({ error: "This request has already been reviewed." })
    }

    const user = await User.findById(request.creatorId)

    if (!user) {
      return res.status(404).json({ error: "Creator not found." })
    }

    const cooldownHours = Math.max(24, Math.min(72, Number(req.body?.cooldownHours) || 48))
    const cooldownUntil = new Date(Date.now() + cooldownHours * 3_600_000)

    user.payoutProfile = {
      ...request.requestedProfile,
      locked: true,
      status: "verified",
      verifiedAt: new Date(),
      lockedAt: new Date(),
      changeRequiresSupport: true,
    }
    await user.save()

    request.status = "approved"
    request.reviewedBy = req.adminSession._id.toString()
    request.reviewedAt = new Date()
    request.cooldownUntil = cooldownUntil
    await request.save()

    await createAuditLog({
      actorType: "admin",
      actorId: req.adminSession._id.toString(),
      eventType: "portal.payout_profile_change.approved",
      message: `Admin approved payout profile change for ${user.email}.`,
      metadata: {
        requestId: request._id.toString(),
        userId: user._id.toString(),
        cooldownUntil,
      },
    })

    res.json({ request: sanitizePayoutProfileChangeRequest(request), user: sanitizeUser(user) })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to approve payout profile change." })
  }
})

app.post("/portal/payout-profile-change-requests/:id/reject", requireAdminSession, async (req, res) => {
  try {
    const request = await PayoutProfileChangeRequest.findById(req.params.id)

    if (!request) {
      return res.status(404).json({ error: "Change request not found." })
    }

    if (request.status !== "awaiting_review") {
      return res.status(409).json({ error: "This request has already been reviewed." })
    }

    const reason = String(req.body?.reason || "").trim()

    if (!reason) {
      return res.status(400).json({ error: "A rejection reason is required." })
    }

    request.status = "rejected"
    request.reviewedBy = req.adminSession._id.toString()
    request.reviewedAt = new Date()
    request.rejectionReason = reason
    await request.save()

    await createAuditLog({
      actorType: "admin",
      actorId: req.adminSession._id.toString(),
      eventType: "portal.payout_profile_change.rejected",
      message: "Admin rejected a payout profile change request.",
      metadata: {
        requestId: request._id.toString(),
        creatorId: request.creatorId.toString(),
        reason,
      },
    })

    res.json({ request: sanitizePayoutProfileChangeRequest(request) })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to reject payout profile change." })
  }
})

app.post("/portal/payouts/reconcile", requireAdminSession, async (req, res) => {
  try {
    const result = await reconcilePendingPayouts()
    await createAuditLog({
      actorType: "admin",
      actorId: req.adminSession._id.toString(),
      eventType: "portal.payouts.reconciled",
      message: "Admin triggered payout reconciliation.",
      metadata: result,
    })
    res.json(result)
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to reconcile payouts." })
  }
})

app.post("/portal/payouts/release-stuck-pending", requireAdminSession, async (req, res) => {
  try {
    const reason =
      String(req.body?.reason || "").trim() ||
      "Legacy auto-disbursement stayed pending at Monnify authorization. Marked failed so creator balance is released."
    const stuckPayouts = await Payout.find({
      status: "pending",
      reviewStatus: "not_required",
    }).sort({ createdAt: 1 })
    let updated = 0

    for (const payout of stuckPayouts) {
      await failPayoutAndReleaseBalance({
        payout,
        actorId: req.adminSession._id.toString(),
        reason,
        eventType: "portal.payout.legacy_pending_failed",
      })
      updated += 1
    }

    await createAuditLog({
      actorType: "admin",
      actorId: req.adminSession._id.toString(),
      eventType: "portal.payouts.legacy_pending_release_completed",
      message: "Admin released legacy auto-pending payout balances.",
      metadata: {
        checked: stuckPayouts.length,
        updated,
        reason,
      },
    })

    res.json({ checked: stuckPayouts.length, updated })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to release stuck pending payouts." })
  }
})

app.post("/portal/payouts/:id/fail", requireAdminSession, async (req, res) => {
  try {
    const payout = await Payout.findById(req.params.id)

    if (!payout) {
      return res.status(404).json({ error: "Payout not found." })
    }

    if (!["pending", "awaiting_review", "failed"].includes(payout.status)) {
      return res.status(409).json({ error: "Only pending, review, or failed payouts can be marked failed." })
    }

    if (payoutRequiresMonnifyAuthorization(payout) && req.body?.confirmRelease !== true) {
      return res.status(400).json({
        error: "This payout is waiting for Monnify OTP. Retry the transfer, or explicitly confirm balance release.",
      })
    }

    const reason =
      String(req.body?.reason || "").trim() ||
      "Payout failed or expired at provider authorization. Creator balance released."
    const updatedPayout = await failPayoutAndReleaseBalance({
      payout,
      actorId: req.adminSession._id.toString(),
      reason,
    })

    res.json({ payout: updatedPayout })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to mark payout failed." })
  }
})

app.post("/portal/payouts/:id/cancel", requireAdminSession, async (req, res) => {
  try {
    const payout = await Payout.findById(req.params.id)

    if (!payout) {
      return res.status(404).json({ error: "Payout not found." })
    }

    if (!["pending", "awaiting_review", "failed"].includes(payout.status)) {
      return res.status(409).json({ error: "Only pending, review, or failed payouts can be cancelled." })
    }

    if (payoutRequiresMonnifyAuthorization(payout) && req.body?.confirmCancel !== true) {
      return res.status(400).json({
        error: "This payout is waiting for Monnify OTP. Retry the transfer, or explicitly confirm cancellation.",
      })
    }

    const reason = String(req.body?.reason || "").trim()

    payout.status = "cancelled"
    payout.reviewStatus = payout.reviewStatus === "awaiting_review" ? "rejected" : payout.reviewStatus
    payout.reviewedBy = req.adminSession._id.toString()
    payout.reviewedAt = new Date()
    payout.rejectionReason = reason || "Cancelled after provider expiry."
    payout.providerMessage = payout.rejectionReason
    payout.completedAt = new Date()
    await payout.save()

    io.to(getCreatorRoom(payout.creatorId)).emit("newPayout", payout)

    await createAuditLog({
      actorType: "admin",
      actorId: req.adminSession._id.toString(),
      eventType: "portal.payout.cancelled",
      message: `Admin cancelled payout ${payout.transferReference}.`,
      metadata: {
        payoutId: payout._id.toString(),
        creatorId: payout.creatorId.toString(),
        amount: payout.amount,
        reason: payout.rejectionReason,
      },
    })

    res.json({ payout })
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to cancel payout." })
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
      GiftSound.deleteMany({ ownerId: user._id }),
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
      firstName = "",
      middleName = "",
      lastName = "",
      email,
      password,
      role = "creator",
      status = "active",
      provisionVirtualAccount = false,
    } = req.body || {}

    const normalizedEmail = String(email || "").toLowerCase().trim()
    const fallbackParts = splitNamePartsFromFullName(name)
    const legal = validateLegalNameParts({
      firstName: firstName || fallbackParts.firstName,
      middleName: middleName || fallbackParts.middleName,
      lastName: lastName || fallbackParts.lastName,
    })
    const trimmedName = buildFullNameFromParts({
      firstName: legal.first,
      middleName: legal.middle,
      lastName: legal.last,
    })
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
      firstName: legal.first,
      middleName: legal.middle,
      lastName: legal.last,
      email: normalizedEmail,
      passwordHash: hashPassword(rawPassword),
      sessionToken: null,
      role: nextRole,
      status: nextStatus,
      identity: {
        firstName: legal.first,
        middleName: legal.middle,
        lastName: legal.last,
      },
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

    const fallbackParts =
      typeof req.body?.name === "string" ? splitNamePartsFromFullName(req.body.name) : {}
    const currentNameParts = getUserNameParts(user)
    const legal = validateLegalNameParts({
      firstName:
        typeof req.body?.firstName === "string"
          ? req.body.firstName
          : fallbackParts.firstName || currentNameParts.firstName,
      middleName:
        typeof req.body?.middleName === "string"
          ? req.body.middleName
          : fallbackParts.middleName || currentNameParts.middleName,
      lastName:
        typeof req.body?.lastName === "string"
          ? req.body.lastName
          : fallbackParts.lastName || currentNameParts.lastName,
    })
    const nextEmail =
      typeof req.body?.email === "string" ? req.body.email.toLowerCase().trim() : user.email
    const nextRole = req.body?.role === "admin" ? "admin" : req.body?.role === "creator" ? "creator" : user.role
    const nextStatus = ["active", "suspended", "banned"].includes(String(req.body?.status))
      ? String(req.body.status)
      : user.status

    if (!nextEmail) {
      return res.status(400).json({ error: "Email is required." })
    }

    const existingUser = await User.findOne({
      email: nextEmail,
      _id: { $ne: user._id },
    })

    if (existingUser) {
      return res.status(409).json({ error: "Another user already uses that email." })
    }

    applyUserNameParts(user, {
      firstName: legal.first,
      middleName: legal.middle,
      lastName: legal.last,
    })
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

app.post("/payout-profile", requireSessionUser, async (req, res) => {
  try {
    if (req.user.payoutProfile?.locked) {
      return res.status(409).json({
        error:
          "Your payout account is locked. Contact support with government ID and NIN proof to request a change.",
      })
    }

    const payoutProfile = await buildVerifiedPayoutProfile({
      user: req.user,
      bankCode: req.body?.bankCode,
      bankName: req.body?.bankName,
      accountNumber: req.body?.accountNumber,
      firstName: req.body?.firstName,
      middleName: req.body?.middleName,
      lastName: req.body?.lastName,
    })

    applyUserNameParts(req.user, {
      firstName: payoutProfile.firstName,
      middleName: payoutProfile.middleName,
      lastName: payoutProfile.lastName,
    })
    req.user.payoutProfile = payoutProfile
    await req.user.save()

    await createAuditLog({
      actorType: "user",
      actorId: req.user._id.toString(),
      eventType: "payout_profile.locked",
      message: `${req.user.email} locked a verified payout bank account.`,
      metadata: {
        bankName: payoutProfile.bankName,
        bankCode: payoutProfile.bankCode,
        accountNumberLast4: payoutProfile.accountNumber.slice(-4),
        accountName: payoutProfile.accountName,
      },
    })

    res.json({ user: sanitizeUser(req.user), payoutProfile: sanitizePayoutProfile(req.user.payoutProfile) })
  } catch (error) {
    console.error(error)
    res.status(400).json({
      error:
        error instanceof Error ? error.message : "Could not verify and lock this payout account.",
    })
  }
})

app.post("/payout-profile/change-request", requireSessionUser, async (req, res) => {
  try {
    if (!req.user.payoutProfile?.locked) {
      return res.status(400).json({
        error: "Add and lock your first payout profile before requesting a support change.",
      })
    }

    const existing = await PayoutProfileChangeRequest.findOne({
      creatorId: req.user._id,
      status: "awaiting_review",
    })

    if (existing) {
      return res.status(409).json({
        error: "You already have a payout account change request awaiting review.",
      })
    }

    const requestedProfile = await buildVerifiedPayoutProfile({
      user: req.user,
      bankCode: req.body?.bankCode,
      bankName: req.body?.bankName,
      accountNumber: req.body?.accountNumber,
      firstName: req.body?.firstName,
      middleName: req.body?.middleName,
      lastName: req.body?.lastName,
    })
    const supportNote = String(req.body?.supportNote || "").trim()
    const proofSummary = String(req.body?.proofSummary || "").trim()

    if (!supportNote || !proofSummary) {
      return res.status(400).json({
        error:
          "Explain why the account must change and list the government ID/NIN proof you will provide to support.",
      })
    }

    const request = await PayoutProfileChangeRequest.create({
      creatorId: req.user._id,
      currentProfile: sanitizePayoutProfile(req.user.payoutProfile),
      requestedProfile,
      supportNote,
      proofSummary,
      status: "awaiting_review",
      createdAt: new Date(),
    })

    await createAuditLog({
      actorType: "user",
      actorId: req.user._id.toString(),
      eventType: "payout_profile.change_requested",
      message: `${req.user.email} requested a payout account change.`,
      metadata: {
        requestId: request._id.toString(),
        requestedBankName: requestedProfile.bankName,
        requestedAccountLast4: requestedProfile.accountNumber.slice(-4),
      },
    })

    res.status(201).json({ request: sanitizePayoutProfileChangeRequest(request) })
  } catch (error) {
    console.error(error)
    res.status(400).json({
      error:
        error instanceof Error ? error.message : "Could not submit payout profile change request.",
    })
  }
})

app.delete("/admin/users/:id/virtual-account", requireAdminSession, async (req, res) => {
  try {
    const user = await User.findById(req.params.id)

    if (!user) {
      return res.status(404).json({ error: "User not found." })
    }

    if (!user.virtualAccount?.accountNumber) {
      return res.status(404).json({ error: "This user does not have a saved virtual account." })
    }

    const { user: updatedUser, previousVirtualAccount, remoteDeallocation } =
      await removeVirtualAccountForUser(user)

    await createAuditLog({
      actorType: "admin",
      actorId: req.adminSession._id.toString(),
      eventType: "admin.user.virtual_account.deleted",
      message: `Admin deleted the saved virtual account for ${user.email}.`,
      metadata: {
        userId: user._id.toString(),
        email: user.email,
        accountReference: previousVirtualAccount?.accountReference || "",
        accountNumberLast4: String(previousVirtualAccount?.accountNumber || "").slice(-4),
        bankName: previousVirtualAccount?.bankName || "",
        remoteDeallocation,
      },
    })

    res.json({
      user: sanitizeUser(updatedUser),
      virtualAccount: null,
      remoteDeallocation,
    })
  } catch (error) {
    console.error(error)
    res.status(502).json({
      error:
        error instanceof Error
          ? error.message
          : "Could not delete the virtual account.",
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

setInterval(() => {
  void reconcilePendingPayouts().catch((error) => {
    console.error("payout.reconciliation.interval_failed", error)
  })
}, 5 * 60 * 1000)
