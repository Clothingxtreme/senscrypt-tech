// Read-only audit of referral codes before any index change. It never writes to the database.
//
//   node scripts/audit-referral-codes.js            (uses MONGODB_URI from .env)
//
// Reports:
//   - indexes on users that involve referralCode / referralCodeAliases
//   - creators sharing the same current referral code (would block a unique index)
//   - creators without a referral code ("" or missing)
//   - codes held by one creator as current code and by another as an alias
//   - referralcodeclaims entries that no creator currently holds (harmless, informational)
// Exit code 1 when duplicates or conflicts are found, 0 otherwise.

const { MongoClient } = require("mongodb")
const path = require("path")

require("dotenv").config({ path: path.join(__dirname, "..", ".env") })

async function main() {
  const uri = String(process.env.MONGODB_URI || "").trim()
  if (!uri) {
    console.error("MONGODB_URI is not set.")
    process.exit(2)
  }

  const client = await MongoClient.connect(uri)
  try {
    const db = client.db()
    const users = db.collection("users")

    const indexes = (await users.indexes()).filter((index) =>
      Object.keys(index.key || {}).some((key) => key.startsWith("referralCode")),
    )
    console.log("Indexes involving referral codes:")
    indexes.forEach((index) =>
      console.log(
        `  ${index.name} ${JSON.stringify(index.key)} unique=${Boolean(index.unique)} sparse=${Boolean(index.sparse)}` +
          (index.partialFilterExpression ? ` partial=${JSON.stringify(index.partialFilterExpression)}` : ""),
      ),
    )

    const duplicateCurrent = await users
      .aggregate([
        { $match: { referralCode: { $type: "string", $gt: "" } } },
        { $group: { _id: { $toUpper: "$referralCode" }, count: { $sum: 1 }, ids: { $push: "$_id" } } },
        { $match: { count: { $gt: 1 } } },
      ])
      .toArray()
    console.log(`\nDuplicate current codes: ${duplicateCurrent.length}`)
    duplicateCurrent.slice(0, 50).forEach((row) => console.log(`  ${row._id} x${row.count} -> ${row.ids.join(", ")}`))

    const missing = await users.countDocuments({
      $or: [{ referralCode: { $exists: false } }, { referralCode: null }, { referralCode: "" }],
    })
    console.log(`\nCreators without a referral code: ${missing}`)

    const aliasConflicts = await users
      .aggregate([
        { $match: { referralCodeAliases: { $exists: true, $ne: [] } } },
        { $unwind: "$referralCodeAliases" },
        {
          $lookup: {
            from: "users",
            let: { alias: "$referralCodeAliases", owner: "$_id" },
            pipeline: [
              {
                $match: {
                  $expr: {
                    $and: [
                      { $ne: ["$_id", "$$owner"] },
                      {
                        $or: [
                          { $eq: ["$referralCode", "$$alias"] },
                          { $in: ["$$alias", { $ifNull: ["$referralCodeAliases", []] }] },
                        ],
                      },
                    ],
                  },
                },
              },
              { $project: { _id: 1 } },
            ],
            as: "others",
          },
        },
        { $match: { others: { $ne: [] } } },
        { $project: { alias: "$referralCodeAliases", owner: "$_id", others: "$others._id" } },
      ])
      .toArray()
    console.log(`\nCodes held by more than one creator via aliases: ${aliasConflicts.length}`)
    aliasConflicts.slice(0, 50).forEach((row) => console.log(`  ${row.alias}: ${row.owner} and ${row.others.join(", ")}`))

    const claims = db.collection("referralcodeclaims")
    const claimCount = await claims.countDocuments()
    const orphanClaims = await claims
      .aggregate([
        {
          $lookup: {
            from: "users",
            let: { code: "$_id" },
            pipeline: [
              {
                $match: {
                  $expr: {
                    $or: [
                      { $eq: ["$referralCode", "$$code"] },
                      { $in: ["$$code", { $ifNull: ["$referralCodeAliases", []] }] },
                    ],
                  },
                },
              },
              { $project: { _id: 1 } },
            ],
            as: "holders",
          },
        },
        { $match: { holders: [] } },
        { $count: "count" },
      ])
      .toArray()
    console.log(`\nReferral code claims: ${claimCount} (not held by any creator: ${orphanClaims[0]?.count || 0})`)

    const problems = duplicateCurrent.length + aliasConflicts.length
    console.log(problems ? "\nResult: conflicts found. Resolve them before adding a unique index." : "\nResult: no conflicts.")
    process.exitCode = problems ? 1 : 0
  } finally {
    await client.close()
  }
}

main().catch((error) => {
  console.error(error)
  process.exit(2)
})
