#!/usr/bin/env bash
# Live demo driver for the order-saga stack.
# Usage:
#   ./demo.sh up       — start the full stack (app + observability) and wait until ready
#   ./demo.sh orders   — fire the three demo scenarios and show their final statuses
#   ./demo.sh poison   — send a poison message to orders.events and show it landing in the DLT
#   ./demo.sh urls     — print every UI/endpoint worth showing
#   ./demo.sh down     — tear everything down
set -euo pipefail

ORDER_API="http://localhost:8081"
VIEW_API="http://localhost:8086"

up() {
  docker compose --profile app --profile obs up -d --build
  echo "Waiting for order-service..."
  for i in $(seq 1 60); do
    if curl -sf -o /dev/null -X POST "$ORDER_API/orders" -H 'Content-Type: application/json' \
        -d '{"customerId":"warmup","items":[{"productId":"P100","quantity":1,"unitPrice":1.00}]}'; then
      echo "Stack is ready."
      urls
      return
    fi
    sleep 2
  done
  echo "order-service did not come up — check: docker compose logs order-service | tail -30" >&2
  exit 1
}

place_order() { # args: label, json body
  local label="$1" body="$2"
  local id
  id=$(curl -s -X POST "$ORDER_API/orders" -H 'Content-Type: application/json' -d "$body" | sed -E 's/.*"orderId":"([^"]+)".*/\1/')
  echo "$label  orderId=$id"
  echo "$id"
}

orders() {
  echo "1) Happy path — 2x P100 @ 49.90 (expect CONFIRMED)"
  HAPPY=$(place_order "   placed:" '{"customerId":"demo","items":[{"productId":"P100","quantity":2,"unitPrice":49.90}]}' | tail -1)
  echo "2) Payment declined — 3x P100 @ 5000.00, total 15000 >= PSP threshold (expect CANCELLED: declined by PSP)"
  DECLINED=$(place_order "   placed:" '{"customerId":"demo","items":[{"productId":"P100","quantity":3,"unitPrice":5000.00}]}' | tail -1)
  echo "3) Out of stock — 50x P200 but only 5 seeded (expect CANCELLED: out of stock, payment refunded)"
  OOS=$(place_order "   placed:" '{"customerId":"demo","items":[{"productId":"P200","quantity":50,"unitPrice":10.00}]}' | tail -1)

  echo
  echo "Waiting for the sagas to settle..."
  sleep 12
  for id in "$HAPPY" "$DECLINED" "$OOS"; do
    echo "--- $ORDER_API/orders/$id"
    curl -s "$ORDER_API/orders/$id"; echo
    echo "--- $VIEW_API/orders/$id/timeline   (CQRS read model, Kafka Streams)"
    curl -s "$VIEW_API/orders/$id/timeline"; echo; echo
  done
  echo "Now open Jaeger and show ONE trace spanning the whole saga:"
  echo "  http://localhost:16686/search?service=order-service&operation=http%20post%20%2Forders"
}

poison() {
  echo "Sending non-Avro garbage to orders.events (no schema-registry magic byte)..."
  docker compose exec -T kafka bash -c \
    'echo "this is not avro" | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:19092 --topic orders.events'
  echo "Deserialization fails fast (no retry for poison) -> routed to payment-service.DLT with failure headers."
  sleep 6
  echo "--- payment-service.DLT contents:"
  docker compose exec -T kafka bash -c \
    '/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic payment-service.DLT --from-beginning --timeout-ms 5000 --property print.headers=true' \
    2>/dev/null | tail -5 || true
}

urls() {
  cat <<EOF

Demo surfaces:
  Order API      $ORDER_API/orders                (POST to create, GET /orders/{id})
  Timeline API   $VIEW_API/orders/{id}/timeline   (Kafka Streams interactive query)
  Jaeger         http://localhost:16686           (one trace across the whole saga)
  Prometheus     http://localhost:9090/targets    (5 services scraped)
  Grafana        http://localhost:3000/d/order-saga
  Registry       http://localhost:8085/subjects   (Avro subjects, RecordNameStrategy)
EOF
}

down() { docker compose --profile app --profile obs down; }

"${1:-urls}"
