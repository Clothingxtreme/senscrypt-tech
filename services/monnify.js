const axios = require("axios")

const MONNIFY_DEFAULT_BASE_URL = "https://api.monnify.com"
const DEFAULT_NIN_VERIFICATION_PATH = "/api/v1/vas/nin-verification"
const LEGACY_NIN_VERIFICATION_PATH = "/api/v1/vas/nin-details"

function normalizeBaseUrl(value) {
  return String(value || MONNIFY_DEFAULT_BASE_URL).replace(/\/+$/, "")
}

function normalizePath(value, fallback) {
  const path = String(value || fallback || "").trim()

  if (!path) {
    return fallback
  }

  return path.startsWith("/") ? path : `/${path}`
}

function createMonnifyService({
  apiKey,
  secretKey,
  contractCode,
  baseUrl,
  ninVerificationPath,
} = {}) {
  const normalizedApiKey = String(apiKey || "").trim()
  const normalizedSecretKey = String(secretKey || "").trim()
  const normalizedContractCode = String(contractCode || "").trim()
  const normalizedBaseUrl = normalizeBaseUrl(baseUrl)
  const normalizedNinVerificationPath = normalizePath(
    ninVerificationPath,
    DEFAULT_NIN_VERIFICATION_PATH,
  )

  let cachedToken = ""
  let cachedTokenExpiresAt = 0

  function ensureConfigured() {
    if (!normalizedApiKey || !normalizedSecretKey || !normalizedBaseUrl) {
      throw new Error(
        "Monnify is not configured yet. Add MONNIFY_API_KEY, MONNIFY_SECRET_KEY, and MONNIFY_BASE_URL to Backend/.env.",
      )
    }
  }

  async function getAccessToken() {
    ensureConfigured()

    if (cachedToken && cachedTokenExpiresAt > Date.now() + 30_000) {
      return cachedToken
    }

    const credentials = Buffer.from(`${normalizedApiKey}:${normalizedSecretKey}`).toString("base64")
    const response = await axios.post(
      `${normalizedBaseUrl}/api/v1/auth/login`,
      {},
      {
        headers: {
          Authorization: `Basic ${credentials}`,
          Accept: "application/json",
        },
        timeout: 20_000,
      },
    )

    const responseBody = response.data?.responseBody || {}
    const accessToken = String(responseBody.accessToken || "").trim()
    const expiresIn = Number(responseBody.expiresIn) || 0

    if (!accessToken) {
      throw new Error("Monnify did not return an access token.")
    }

    cachedToken = accessToken
    cachedTokenExpiresAt = Date.now() + Math.max(expiresIn - 60, 60) * 1000

    return cachedToken
  }

  async function request(method, path, { data, params } = {}) {
    const accessToken = await getAccessToken()
    const url = `${normalizedBaseUrl}${normalizePath(path, path)}`

    return axios({
      method,
      url,
      data,
      params,
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      timeout: 30_000,
    })
  }

  return {
    isConfigured() {
      return Boolean(normalizedApiKey && normalizedSecretKey && normalizedBaseUrl)
    },

    getEnvironment() {
      if (!normalizedBaseUrl) {
        return "unconfigured"
      }

      return normalizedBaseUrl.toLowerCase().includes("sandbox.monnify.com") ? "sandbox" : "live"
    },

    getContractCode() {
      return normalizedContractCode
    },

    getAccessToken,

    async verifyBvnDetails(payload) {
      const response = await request("post", "/api/v1/vas/bvn-details-match", { data: payload })
      return response.data
    },

    async verifyNin(payload) {
      try {
        const response = await request("post", normalizedNinVerificationPath, { data: payload })
        return response.data
      } catch (error) {
        const shouldRetryLegacyPath =
          error?.response?.status === 404 &&
          normalizedNinVerificationPath !== LEGACY_NIN_VERIFICATION_PATH

        if (!shouldRetryLegacyPath) {
          throw error
        }

        const response = await request("post", LEGACY_NIN_VERIFICATION_PATH, { data: payload })
        return response.data
      }
    },

    async updateReservedAccountKyc({ accountReference, bvn, nin }) {
      const response = await request(
        "put",
        `/api/v1/bank-transfer/reserved-accounts/${encodeURIComponent(accountReference)}/kyc-info`,
        {
          data: {
            ...(bvn ? { bvn } : {}),
            ...(nin ? { nin } : {}),
          },
        },
      )
      return response.data
    },
  }
}

module.exports = {
  MONNIFY_DEFAULT_BASE_URL,
  DEFAULT_NIN_VERIFICATION_PATH,
  LEGACY_NIN_VERIFICATION_PATH,
  createMonnifyService,
}
