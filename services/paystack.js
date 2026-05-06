const axios = require("axios")

const PAYSTACK_DEFAULT_BASE_URL = "https://api.paystack.co"

function normalizeBaseUrl(value) {
  return String(value || PAYSTACK_DEFAULT_BASE_URL).replace(/\/+$/, "")
}

function createPaystackService({ secretKey, baseUrl } = {}) {
  const normalizedSecretKey = String(secretKey || "").trim()
  const normalizedBaseUrl = normalizeBaseUrl(baseUrl)

  function ensureConfigured() {
    if (!normalizedSecretKey) {
      throw new Error("Paystack is not configured yet. Add PAYSTACK_SECRET_KEY to Backend/.env.")
    }
  }

  async function request(method, path, { data, params } = {}) {
    ensureConfigured()

    const response = await axios({
      method,
      url: `${normalizedBaseUrl}${path}`,
      data,
      params,
      headers: {
        Authorization: `Bearer ${normalizedSecretKey}`,
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      timeout: 20_000,
    })

    return response.data
  }

  return {
    isConfigured() {
      return Boolean(normalizedSecretKey)
    },

    getEnvironment() {
      return normalizedSecretKey.startsWith("sk_live_") ? "live" : "sandbox"
    },

    async createCustomer(payload) {
      return request("post", "/customer", { data: payload })
    },

    async updateCustomer(customerCode, payload) {
      return request("put", `/customer/${encodeURIComponent(customerCode)}`, { data: payload })
    },

    async validateCustomer(customerCode, payload) {
      return request("post", `/customer/${encodeURIComponent(customerCode)}/identification`, {
        data: payload,
      })
    },

    async createDedicatedVirtualAccount(payload) {
      return request("post", "/dedicated_account", { data: payload })
    },

    async assignDedicatedVirtualAccount(payload) {
      return request("post", "/dedicated_account/assign", { data: payload })
    },

    async fetchDedicatedVirtualAccount(accountId) {
      return request("get", `/dedicated_account/${encodeURIComponent(accountId)}`)
    },
  }
}

module.exports = {
  PAYSTACK_DEFAULT_BASE_URL,
  createPaystackService,
}
