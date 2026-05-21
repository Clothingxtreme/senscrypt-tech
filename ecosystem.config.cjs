const socketClusterModeRaw = String(process.env.SOCKET_CLUSTER_MODE ?? "true").trim()
const socketClusterMode = !/^(0|false|no)$/i.test(socketClusterModeRaw)
const socketClusterInstances = process.env.SOCKET_CLUSTER_INSTANCES || "max"

module.exports = {
  apps: [
    {
      name: "streamtip-api",
      script: "server.js",
      cwd: __dirname,
      instances: socketClusterMode ? socketClusterInstances : 1,
      exec_mode: socketClusterMode ? "cluster" : "fork",
      instance_var: "NODE_APP_INSTANCE",
      env: {
        NODE_ENV: "production",
        PORT: process.env.PORT || 5000,
      },
      max_memory_restart: "512M",
      max_restarts: 20,
      restart_delay: 2000,
      kill_timeout: 5000,
      listen_timeout: 10000,
      time: true,
    },
  ],
}
