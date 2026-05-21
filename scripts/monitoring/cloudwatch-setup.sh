#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   AWS_REGION=eu-north-1 \
#   LOG_GROUP_NAME=/streamtip/pm2 \
#   ALARM_SNS_TOPIC_ARN=arn:aws:sns:eu-north-1:123456789012:streamtip-alerts \
#   ./scripts/monitoring/cloudwatch-setup.sh

AWS_REGION="${AWS_REGION:-eu-north-1}"
LOG_GROUP_NAME="${LOG_GROUP_NAME:-/streamtip/pm2}"
ALARM_NAMESPACE="${ALARM_NAMESPACE:-StreamTip/Backend}"
ALARM_SNS_TOPIC_ARN="${ALARM_SNS_TOPIC_ARN:-}"
ALARM_PERIOD_SECONDS="${ALARM_PERIOD_SECONDS:-60}"
ALARM_EVAL_PERIODS="${ALARM_EVAL_PERIODS:-1}"

if [[ -z "${ALARM_SNS_TOPIC_ARN}" ]]; then
  echo "Set ALARM_SNS_TOPIC_ARN before running."
  exit 1
fi

put_filter() {
  local filter_name="$1"
  local pattern="$2"
  local metric_name="$3"

  aws logs put-metric-filter \
    --region "${AWS_REGION}" \
    --log-group-name "${LOG_GROUP_NAME}" \
    --filter-name "${filter_name}" \
    --filter-pattern "${pattern}" \
    --metric-transformations \
      metricName="${metric_name}",metricNamespace="${ALARM_NAMESPACE}",metricValue=1,defaultValue=0
}

put_alarm() {
  local alarm_name="$1"
  local metric_name="$2"
  local threshold="$3"

  aws cloudwatch put-metric-alarm \
    --region "${AWS_REGION}" \
    --alarm-name "${alarm_name}" \
    --namespace "${ALARM_NAMESPACE}" \
    --metric-name "${metric_name}" \
    --statistic Sum \
    --period "${ALARM_PERIOD_SECONDS}" \
    --evaluation-periods "${ALARM_EVAL_PERIODS}" \
    --datapoints-to-alarm 1 \
    --threshold "${threshold}" \
    --comparison-operator GreaterThanOrEqualToThreshold \
    --alarm-actions "${ALARM_SNS_TOPIC_ARN}" \
    --treat-missing-data notBreaching
}

echo "Creating metric filters in ${LOG_GROUP_NAME} ..."
put_filter "StreamTipSocketRedisPubErrors" '"socket.redis.pub.error"' "SocketRedisPubErrors"
put_filter "StreamTipSocketRedisSubErrors" '"socket.redis.sub.error"' "SocketRedisSubErrors"
put_filter "StreamTipQueueFailedJobs" '"webhook.queue.monnify.failed"' "QueueFailedJobs"
put_filter "StreamTipWebhookLag" '"monitor.event" "webhook.queue.lag"' "WebhookLagEvents"
put_filter "StreamTipMongoSlowQueries" '"monitor.event" "mongo.slow_query"' "MongoSlowQueryEvents"

echo "Creating alarms ..."
put_alarm "StreamTip-SocketRedisPubErrors" "SocketRedisPubErrors" 3
put_alarm "StreamTip-SocketRedisSubErrors" "SocketRedisSubErrors" 3
put_alarm "StreamTip-QueueFailedJobs" "QueueFailedJobs" 1
put_alarm "StreamTip-WebhookLag" "WebhookLagEvents" 1
put_alarm "StreamTip-MongoSlowQuerySpikes" "MongoSlowQueryEvents" 10

echo "Done."
