module.exports = {
  apps: [
    {
      name: "streamtip-api",
      script: "server.js",
      cwd: __dirname,
      instances: 1,
      exec_mode: "fork",
      env: {
        NODE_ENV: "production",
        PORT: process.env.PORT || 5000,
      },
      max_memory_restart: "512M",
      time: true,
    },
  ],
}
