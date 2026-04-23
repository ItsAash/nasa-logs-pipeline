import gzip
import time
import logging
import sys
from confluent_kafka import Producer
from constants import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, LOG_FILE_PATH

# ─── Logging setup ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("nasa-producer")

# ─── Kafka config ──────────────────────────────────────────────
conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 3,
    "linger.ms": 10
}

MESSAGES_PER_SECOND = 500

producer = Producer(conf)

# ─── Delivery callback ─────────────────────────────────────────
def delivery_report(err, msg):
    if err is not None:
        log.error(f"Delivery failed: {err}")
    # optional success tracking (disabled for performance)

# ─── Main logic ────────────────────────────────────────────────
def produce_logs(limit=5000, batch_size=1000):
    sent_count = 0
    skipped_count = 0

    start_time = time.time()

    delay = (1.0 / MESSAGES_PER_SECOND) if MESSAGES_PER_SECOND else 0
    try:
        with gzip.open(LOG_FILE_PATH, "rt") as f:
            for line in f:
                # safety limit for testing
                if sent_count >= limit:
                    break
                
                if line is None or line.strip() == "":
                    skipped_count += 1
                    continue

                producer.produce(
                    topic=KAFKA_TOPIC,
                    value=line.strip(),
                    callback=delivery_report
                )

                # flush in small bursts (prevents memory buildup)
                producer.poll(0)

                sent_count += 1

                # progress logging
                if sent_count % batch_size == 0:
                    elapsed = time.time() - start_time
                    rate = sent_count / elapsed
                    log.info(
                        f"Sent {sent_count:,} messages | "
                        f"Skipped {skipped_count} | "
                        f"Rate: {rate:.0f} msg/sec"
                    )

                # Optional: sleep to control throughput
                if delay > 0:
                    time.sleep(delay)

                
                
    except KeyboardInterrupt:
        log.warning("Interrupted by user. Flushing remaining messages...")
        producer.flush()

    except Exception as e:
        log.exception(f"Producer failed: {e}")
        sys.exit(1)

    finally:
      log.info("Flushing remaining messages to Kafka...")
      producer.flush()
  
     # Final summary
    elapsed = time.time() - start_time
    log.info("─" * 50)
    log.info(f"Done! Summary:")
    log.info(f"  Total sent:    {sent_count:,}")
    log.info(f"  Skipped:       {skipped_count:,}")
    log.info(f"  Time elapsed:  {elapsed:.1f}s")
    log.info(f"  Avg rate:      {sent_count / elapsed:.0f} msg/sec")
    log.info("─" * 50)


# ─── Entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Starting Kafka producer...")
    log.info(f"Topic: {KAFKA_TOPIC}")
    log.info(f"File: {LOG_FILE_PATH}")

    produce_logs()