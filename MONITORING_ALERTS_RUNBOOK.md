# StreamTip Backend Monitoring + Alerts

This runbook covers CloudWatch + PM2/log alerts for:
- `socket.redis.*error`
- queue failed jobs
- webhook lag
- Mongo slow query spikes

## 1) Backend env (required)
Add these to `Backend/.env` on the server:

```env
MONITORING_ALERTS_ENABLED=true
MONITORING_ALERT_CHAT_ID=-100xxxxxxxxxx
MONITORING_ALERT_COOLDOWN_MS=300000
MONITORING_ALERT_WINDOW_MS=300000
SOCKET_REDIS_ERROR_ALERT_THRESHOLD=5
WEBHOOK_QUEUE_FAILED_ALERT_THRESHOLD=3
WEBHOOK_LAG_ALERT_MS=30000
MONGO_SLOW_QUERY_THRESHOLD_MS=700
MONGO_SLOW_QUERY_ALERT_THRESHOLD=20
MONITORING_HEARTBEAT_SECONDS=60
```

Then restart:

```bash
pm2 restart streamtip-api --update-env
pm2 save
```

## 2) What backend now emits

The API now emits structured monitor logs as:
- `monitor.event {"eventType":"socket.redis.error", ...}`
- `monitor.event {"eventType":"webhook.queue.failed_job", ...}`
- `monitor.event {"eventType":"webhook.queue.lag", ...}`
- `monitor.event {"eventType":"mongo.slow_query", ...}`
- `monitor.event {"eventType":"monitor.heartbeat", ...}` every `MONITORING_HEARTBEAT_SECONDS`

And sends Telegram alerts (throttled) when thresholds are crossed.

## 3) PM2 log watcher alerts (optional, extra safety)

Run:

```bash
chmod +x ./scripts/monitoring/pm2-log-alert-watch.sh
MONITORING_TELEGRAM_BOT_TOKEN="$TELEGRAM_BOT_TOKEN" \
MONITORING_ALERT_CHAT_ID="$MONITORING_ALERT_CHAT_ID" \
APP_NAME=streamtip-api \
./scripts/monitoring/pm2-log-alert-watch.sh
```

To keep it alive under PM2:

```bash
pm2 start ./scripts/monitoring/pm2-log-alert-watch.sh --name streamtip-log-alerts --interpreter bash
pm2 save
```

## 4) CloudWatch setup

### 4.1 Ensure PM2 logs are shipped to CloudWatch Logs
Ship at minimum:
- `/home/ubuntu/.pm2/logs/streamtip-api-out-*.log`
- `/home/ubuntu/.pm2/logs/streamtip-api-error-*.log`

### 4.2 Create metric filters + alarms
Create an SNS topic first, then run:

```bash
chmod +x ./scripts/monitoring/cloudwatch-setup.sh
AWS_REGION=eu-north-1 \
LOG_GROUP_NAME=/streamtip/pm2 \
ALARM_SNS_TOPIC_ARN=arn:aws:sns:eu-north-1:123456789012:streamtip-alerts \
./scripts/monitoring/cloudwatch-setup.sh
```

## 5) Validate

1. Check app health:
```bash
curl -s https://api.streamtips.live/health
```

2. Check pipeline health:
```bash
curl -s https://api.streamtips.live/admin/pipeline/health -H "x-admin-token: <TOKEN>"
```

3. Force-test alerts:
- Temporarily break Redis SG rule (expect `socket.redis.*error` alerts)
- Push bad webhook jobs (expect `webhook.queue.monnify.failed`)
- Burst queue and watch for `webhook.queue.lag`
- Run heavy query load and watch `mongo.slow_query`

