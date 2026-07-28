#!/usr/bin/env bash

set -euo pipefail

kubeconfig_path="${KUBECONFIG_PATH:-$HOME/.kube/62}"
workspace_namespace="${WORKSPACE_NAMESPACE:-ns-admin}"
cluster_name="${CLUSTER_NAME:-kafka-dev}"
kafka_version="${KAFKA_VERSION:-3.9.0}"
operator_version="${STRIMZI_VERSION:-0.46.0}"
job_name="mq-${cluster_name}-roundtrip-$(date +%s)"
client_name="${cluster_name}-client"
bootstrap_service="${cluster_name}-kafka-bootstrap:9092"

if [[ ! -r "$kubeconfig_path" ]]; then
  echo "kubeconfig is not readable: $kubeconfig_path" >&2
  exit 1
fi

kubectl_cmd=(kubectl --kubeconfig "$kubeconfig_path" -n "$workspace_namespace")

"${kubectl_cmd[@]}" get secret "$client_name" >/dev/null

cat <<EOF | "${kubectl_cmd[@]}" apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
  labels:
    app.kubernetes.io/name: messagequeue-roundtrip
    app.kubernetes.io/instance: ${cluster_name}
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 300
  template:
    metadata:
      labels:
        app.kubernetes.io/name: messagequeue-roundtrip
        app.kubernetes.io/instance: ${cluster_name}
    spec:
      restartPolicy: Never
      containers:
        - name: kafka-client
          image: quay.io/strimzi/kafka:${operator_version}-kafka-${kafka_version}
          imagePullPolicy: IfNotPresent
          env:
            - name: CLIENT_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: ${client_name}
                  key: password
            - name: CLIENT_USERNAME
              value: ${client_name}
            - name: BOOTSTRAP_SERVERS
              value: ${bootstrap_service}
            - name: SMOKE_TOPIC
              value: ${job_name}
            - name: KAFKA_HEAP_OPTS
              value: -Xms64m -Xmx192m
          resources:
            requests:
              cpu: 250m
              memory: 512Mi
            limits:
              cpu: "1"
              memory: 1Gi
          command:
            - /bin/bash
            - -ec
          args:
            - |
              set +x
              umask 077
              client_config="\$(mktemp)"
              consumer_output="\$(mktemp)"
              trap 'rm -f "\$client_config" "\$consumer_output"' EXIT
              cat > "\$client_config" <<CONFIG
              security.protocol=SASL_PLAINTEXT
              sasl.mechanism=SCRAM-SHA-512
              sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="\${CLIENT_USERNAME}" password="\${CLIENT_PASSWORD}";
              CONFIG

              topic="\${SMOKE_TOPIC}"
              message="messagequeue-roundtrip-\$(date +%s%N)"

              /opt/kafka/bin/kafka-topics.sh \
                --bootstrap-server "\${BOOTSTRAP_SERVERS}" \
                --command-config "\$client_config" \
                --create --if-not-exists \
                --topic "\$topic" --partitions 1 --replication-factor 1

              printf '%s\n' "\$message" | /opt/kafka/bin/kafka-console-producer.sh \
                --bootstrap-server "\${BOOTSTRAP_SERVERS}" \
                --producer.config "\$client_config" \
                --topic "\$topic"

              timeout 45 /opt/kafka/bin/kafka-console-consumer.sh \
                --bootstrap-server "\${BOOTSTRAP_SERVERS}" \
                --consumer.config "\$client_config" \
                --from-beginning --topic "\$topic" --max-messages 1 \
                > "\$consumer_output"
              grep -Fx -- "\$message" "\$consumer_output" >/dev/null
              echo "ROUNDTRIP_OK cluster=${cluster_name} topic=\${topic}"
EOF

"${kubectl_cmd[@]}" wait --for=condition=complete "job/$job_name" --timeout=3m
job_logs="$("${kubectl_cmd[@]}" logs "job/$job_name")"
if [[ "$job_logs" != *"ROUNDTRIP_OK cluster=$cluster_name"* ]]; then
  echo "$job_logs" >&2
  echo "Kafka round-trip did not produce the expected success marker" >&2
  exit 1
fi
printf '%s\n' "$job_logs"
