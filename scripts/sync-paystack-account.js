/**
 * Run on the server to fetch a user's dedicated account from Paystack and activate it in MongoDB.
 * Usage: node scripts/sync-paystack-account.js <email>
 * Example: node scripts/sync-paystack-account.js alexadeboye44@gmail.com
 *
 * Requires PAYSTACK_SECRET_KEY and MONGODB_URI in Backend/.env (or environment variables).
 */

require("dotenv").config({ path: require("path").resolve(__dirname, "../.env") })

const axios = require("axios")
const { MongoClient, ObjectId } = require("mongodb")

const MONGODB_URI = process.env.MONGODB_URI
const PAYSTACK_SECRET_KEY = process.env.PAYSTACK_SECRET_KEY
const PAYSTACK_BASE_URL = process.env.PAYSTACK_BASE_URL || "https://api.paystack.co"

const email = process.argv[2]

if (!email) {
  console.error("Usage: node scripts/sync-paystack-account.js <email>")
  process.exit(1)
}

if (!PAYSTACK_SECRET_KEY) {
  console.error("PAYSTACK_SECRET_KEY not set in .env")
  process.exit(1)
}

const headers = { Authorization: `Bearer ${PAYSTACK_SECRET_KEY}` }

async function paystackGet(path, params = {}) {
  const url = `${PAYSTACK_BASE_URL}${path}`
  console.log(`[Paystack] GET ${url}`, Object.keys(params).length ? params : "")
  const r = await axios.get(url, { headers, params, timeout: 20000 })
  return r.data
}

;(async () => {
  const client = new MongoClient(MONGODB_URI)
  await client.connect()
  const db = client.db()
  const users = db.collection("users")

  const user = await users.findOne({ email: email.toLowerCase().trim() })
  if (!user) {
    console.error(`User not found: ${email}`)
    await client.close()
    process.exit(1)
  }

  console.log(`Found user: ${user.name} (${user.email})`)
  console.log(`Phone in DB: ${user.phoneNumber || "NONE"}`)
  console.log(`Current virtualAccount:`, JSON.stringify(user.virtualAccount, null, 2))

  const customerCode = user.virtualAccount?.paystackCustomerCode
  if (!customerCode) {
    console.error("No paystackCustomerCode saved. You need to provision first (or run provision step).")
    await client.close()
    process.exit(1)
  }

  console.log(`\nFetching customer details for ${customerCode}...`)
  const customerRes = await paystackGet(`/customer/${encodeURIComponent(customerCode)}`)
  console.log("Customer response:", JSON.stringify(customerRes, null, 2))

  // Paystack returns dedicated_accounts array inside the customer object
  const customerData = customerRes?.data || {}
  const dedicatedAccounts = customerData.dedicated_accounts || customerData.dedicatedAccounts || []
  const accounts = dedicatedAccounts.filter(a => a.active || a.status === "active" || !a.status)

  if (accounts.length === 0) {
    console.log("\nNo active account found on Paystack yet. Trying assign again...")

    // Build phone in Nigerian format
    const rawPhone = String(user.phoneNumber || "").replace(/[^\d+]/g, "").trim()
    if (!rawPhone) {
      console.error("User has no phone number. Cannot call /assign.")
      await client.close()
      process.exit(1)
    }

    const nameParts = (user.name || "").split(" ").filter(Boolean)
    const firstName = nameParts[0] || "Creator"
    const lastName = nameParts.slice(1).join(" ") || firstName

    const assignPayload = {
      email: user.email,
      first_name: firstName,
      last_name: lastName,
      phone: rawPhone,
      country: "NG",
    }

    // Add split_code or subaccount if configured
    if (process.env.PAYSTACK_DVA_SPLIT_CODE) {
      assignPayload.split_code = process.env.PAYSTACK_DVA_SPLIT_CODE
    } else if (process.env.PAYSTACK_DVA_SUBACCOUNT) {
      assignPayload.subaccount = process.env.PAYSTACK_DVA_SUBACCOUNT
    }

    const assignUrl = `${PAYSTACK_BASE_URL}/dedicated_account/assign`
    console.log(`[Paystack] POST ${assignUrl}`, assignPayload)
    const assignRes = await axios.post(assignUrl, assignPayload, { headers, timeout: 20000 })
    console.log("Assign response:", JSON.stringify(assignRes.data, null, 2))
    console.log("\nAssign queued. Paystack will send a webhook when the account is ready.")
    console.log("Check PM2 logs: pm2 logs streamtip-api --lines 30")
    await client.close()
    process.exit(0)
  }

  const account = accounts[0]
  const accountNumber = String(account.account_number || "").trim()
  const bankName = String(account.bank?.name || "Paystack DVA").trim()
  const bankCode = String(account.bank?.slug || account.bank?.code || "").trim()

  console.log(`\nFound account: ${accountNumber} at ${bankName}`)

  const virtualAccount = {
    accountReference: String(account.id || accountNumber),
    accountName: String(account.account_name || user.name || "").trim(),
    accountNumber,
    bankName,
    bankCode,
    reservationReference: String(account.id || ""),
    status: "active",
    provider: "paystack",
    environment: PAYSTACK_SECRET_KEY.startsWith("sk_live_") ? "live" : "sandbox",
    assignmentStatus: "assigned",
    paystackCustomerCode: customerCode,
    dedicatedAccountId: String(account.id || ""),
    createdAt: account.created_at ? new Date(account.created_at) : new Date(),
    updatedAt: new Date(),
  }

  const result = await users.updateOne(
    { _id: user._id },
    { $set: { virtualAccount } }
  )

  if (result.modifiedCount === 1) {
    console.log(`\nSaved to MongoDB. User ${user.email} virtual account is now ACTIVE.`)
    console.log("Account:", JSON.stringify(virtualAccount, null, 2))
  } else {
    console.error("MongoDB update failed (no documents modified).")
  }

  await client.close()
})().catch((err) => {
  console.error("Error:", err?.response?.data || err.message)
  process.exit(1)
})
