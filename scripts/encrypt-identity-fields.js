require("dotenv").config({ path: require("path").join(__dirname, "..", ".env") })

const crypto = require("crypto")
const { MongoClient } = require("mongodb")

const MONGODB_URI = String(process.env.MONGODB_URI || "").trim()
const IDENTITY_ENCRYPTION_KEY = String(process.env.IDENTITY_ENCRYPTION_KEY || "").trim()

function getIdentityEncryptionKey() {
  if (!IDENTITY_ENCRYPTION_KEY) {
    throw new Error("IDENTITY_ENCRYPTION_KEY is required.")
  }

  if (/^[a-f0-9]{64}$/i.test(IDENTITY_ENCRYPTION_KEY)) {
    return Buffer.from(IDENTITY_ENCRYPTION_KEY, "hex")
  }

  return crypto.createHash("sha256").update(IDENTITY_ENCRYPTION_KEY).digest()
}

function isEncryptedIdentityValue(value) {
  return String(value || "").startsWith("enc:v1:")
}

function encryptIdentityValue(value, key) {
  const plainText = String(value || "").trim()
  if (!plainText || isEncryptedIdentityValue(plainText)) {
    return plainText
  }

  const iv = crypto.randomBytes(12)
  const cipher = crypto.createCipheriv("aes-256-gcm", key, iv)
  const encrypted = Buffer.concat([cipher.update(plainText, "utf8"), cipher.final()])
  const tag = cipher.getAuthTag()

  return [
    "enc:v1",
    iv.toString("base64url"),
    tag.toString("base64url"),
    encrypted.toString("base64url"),
  ].join(":")
}

async function main() {
  if (!MONGODB_URI) {
    throw new Error("MONGODB_URI is required.")
  }

  const key = getIdentityEncryptionKey()
  const client = new MongoClient(MONGODB_URI)
  await client.connect()
  const users = client.db().collection("users")
  const cursor = users.find({
    $or: [
      { "identity.bvn": { $exists: true, $ne: "" } },
      { "identity.nin": { $exists: true, $ne: "" } },
    ],
  })
  let scanned = 0
  let updated = 0

  for await (const user of cursor) {
    scanned += 1
    const bvn = String(user.identity?.bvn || "").trim()
    const nin = String(user.identity?.nin || "").trim()
    const $set = {}

    if (bvn && !isEncryptedIdentityValue(bvn)) {
      $set["identity.bvn"] = encryptIdentityValue(bvn, key)
    }

    if (nin && !isEncryptedIdentityValue(nin)) {
      $set["identity.nin"] = encryptIdentityValue(nin, key)
    }

    if (Object.keys($set).length) {
      await users.updateOne({ _id: user._id }, { $set })
      updated += 1
    }
  }

  await client.close()
  console.log(`Identity encryption migration complete. scanned=${scanned} updated=${updated}`)
}

main().catch((error) => {
  console.error(error.message)
  process.exit(1)
})
