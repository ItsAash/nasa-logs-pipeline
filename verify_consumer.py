
import logging
from confluent_kafka import Consumer
from constants import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

PRINT_EVERY_N = 100

GROUP_ID = "nasa-log-verifier"

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest"
})

consumer.subscribe([KAFKA_TOPIC])

log.info("Consumer started... waiting for messages")

count = 0

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            log.error(msg.error())
            continue

        count += 1

        if count % PRINT_EVERY_N == 0:
            log.info(f"[{count}] {msg.value().decode('utf-8')[:120]}")

except KeyboardInterrupt:
    log.info("Stopping consumer...")

finally:
    consumer.close()