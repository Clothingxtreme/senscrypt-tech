const { MongoClient } = require("mongodb")
const path = require("path")

require("dotenv").config({ path: path.join(__dirname, "..", ".env") })

const PLATFORM_FEE_RATE = 0.2
const CREATOR_SHARE_RATE = 0.8
const MONNIFY_CREATOR_SETTLEMENT_VAT_RATE = 0.0202
const DEFAULT_CUTOFF = "2026-06-20T22:15:00+01:00"

function readArg(name) {
  const prefix = `--${name}=`
  const match = process.argv.find((arg) => arg.startsWith(prefix))
  return match ? match.slice(prefix.length).trim() : ""
}

function roundWalletAmount(value) {
  const amount = Number(value) || 0
  return Math.round(amount * 100) / 100
}

function getCreatorShare(donation) {
  const savedCreatorShare = Number(donation.creatorShare)
  if (Number.isFinite(savedCreatorShare) && savedCreatorShare > 0) {
    return savedCreatorShare
  }

  const gross = Number(donation.amount) || 0
  const platformFee = Math.round(gross * PLATFORM_FEE_RATE)
  return Math.max(0, gross - platformFee)
}

function getCreatorSettlement(donation) {
  const creatorShare = getCreatorShare(donation)
  const savedNet = Number(donation.creatorSettlementNetAmount)
  const savedVat = Number(donation.creatorSettlementVat)
  const creatorSettlementVat =
    Number.isFinite(savedVat) && savedVat > 0
      ? savedVat
      : roundWalletAmount(creatorShare * MONNIFY_CREATOR_SETTLEMENT_VAT_RATE)
  const creatorSettlementNetAmount =
    Number.isFinite(savedNet) && savedNet > 0
      ? savedNet
      : roundWalletAmount(Math.max(0, creatorShare - creatorSettlementVat))

  return {
    creatorShare,
    creatorSettlementVat,
    creatorSettlementNetAmount,
  }
}

async function main() {
  const uri = process.env.MONGODB_URI
  const databaseName = process.env.MONGODB_DATABASE || "streamtip"
  const cutoffText = readArg("cutoff") || process.env.SETTLEMENT_CUTOFF || DEFAULT_CUTOFF
  const cutoff = new Date(cutoffText)
  const confirmed = process.argv.includes("--confirm")

  if (!uri) {
    throw new Error("MONGODB_URI is required.")
  }

  if (Number.isNaN(cutoff.getTime())) {
    throw new Error(`Invalid cutoff date: ${cutoffText}`)
  }

  const client = new MongoClient(uri)
  await client.connect()

  try {
    const donations = client.db(databaseName).collection("donations")
    const filter = {
      provider: "monnify",
      walletStatus: { $ne: "rejected" },
      settlementStatus: { $in: ["pending", "partial", "unknown", "PENDING_SETTLEMENT"] },
      date: { $lt: cutoff },
    }
    const pending = await donations.find(filter).toArray()

    console.log(
      JSON.stringify(
        {
          mode: confirmed ? "write" : "dry-run",
          cutoff: cutoff.toISOString(),
          matched: pending.length,
        },
        null,
        2,
      ),
    )

    if (!confirmed || pending.length === 0) {
      if (!confirmed) {
        console.log("Dry run only. Re-run with --confirm to mark these settlements as settled.")
      }
      return
    }

    const now = new Date()
    const operations = pending.map((donation) => {
      const settlement = getCreatorSettlement(donation)
      const references = Array.isArray(donation.settlementReferences)
        ? donation.settlementReferences
        : []
      const referenceNote = `manual-settled-before-${cutoff.toISOString()}`

      return {
        updateOne: {
          filter: { _id: donation._id },
          update: {
            $set: {
              creatorShare: settlement.creatorShare,
              creatorSettlementVat: settlement.creatorSettlementVat,
              creatorSettlementNetAmount: settlement.creatorSettlementNetAmount,
              creatorSettledAmount: settlement.creatorSettlementNetAmount,
              settlementPendingAmount: 0,
              settlementStatus: "settled",
              settlementSyncedAt: now,
              settlementConfirmedAt: now,
              settlementLastError: "",
              settlementReferences: Array.from(new Set([...references, referenceNote])),
            },
          },
        },
      }
    })

    const result = await donations.bulkWrite(operations, { ordered: false })
    console.log(
      JSON.stringify(
        {
          modified: result.modifiedCount,
          matched: result.matchedCount,
        },
        null,
        2,
      ),
    )
  } finally {
    await client.close()
  }
}

main().catch((error) => {
  console.error(error)
  process.exitCode = 1
})
