const { MongoClient } = require("mongodb")
const path = require("path")

require("dotenv").config({ path: path.join(__dirname, "..", ".env") })

const PLATFORM_FEE_RATE = 0.2
const MONNIFY_CREATOR_SETTLEMENT_VAT_RATE = 0.0161
const LAGOS_TIME_OFFSET_MS = 60 * 60 * 1000

function readArg(name) {
  const prefix = `--${name}=`
  const match = process.argv.find((arg) => arg.startsWith(prefix))
  return match ? match.slice(prefix.length).trim() : ""
}

function roundWalletAmount(value) {
  const amount = Number(value) || 0
  return Math.round(amount * 100) / 100
}

function lagosDateTimeToUtcDate({ year, month, day, hour = 0, minute = 0 }) {
  return new Date(Date.UTC(year, month, day, hour, minute, 0, 0) - LAGOS_TIME_OFFSET_MS)
}

function getDefaultYesterday11pmCutoff() {
  const nowInLagos = new Date(Date.now() + LAGOS_TIME_OFFSET_MS)
  return lagosDateTimeToUtcDate({
    year: nowInLagos.getUTCFullYear(),
    month: nowInLagos.getUTCMonth(),
    day: nowInLagos.getUTCDate() - 1,
    hour: 23,
    minute: 0,
  })
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
  const cutoffText = readArg("cutoff")
  const cutoff = cutoffText ? new Date(cutoffText) : getDefaultYesterday11pmCutoff()
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
      fundsFlow: { $in: ["wallet", "direct_split"] },
      walletStatus: { $ne: "rejected" },
      settlementStatus: { $in: ["pending", "partial", "unknown", "PENDING_SETTLEMENT"] },
      date: { $lte: cutoff },
    }
    const pending = await donations.find(filter).toArray()

    console.log(
      JSON.stringify(
        {
          mode: confirmed ? "write" : "dry-run",
          cutoff: cutoff.toISOString(),
          cutoffMeaning: "Yesterday 11:00pm Africa/Lagos by default; override with --cutoff=ISO_DATE.",
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
      const referenceNote = `manual-yesterday-11pm-settled-before-${cutoff.toISOString()}`

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
