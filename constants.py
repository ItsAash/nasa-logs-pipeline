KAFKA_BOOTSTRAP_SERVERS = "0.tcp.in.ngrok.io:11325"
KAFKA_TOPIC = "nasa-logs"
LOG_FILE_PATH = "./raw-logs/NASA_access_log_Jul95.gz"

# Standard NASA common log format regex
# ^(\S+) -> host
# \S+ \S+ -> Ident/Auth (skipped)
# \[([^\]]+)\] -> timestamp
# "(\S+) (\S+) (\S+)" -> method, endpoint, protocol
# (\d{3}) -> status_code
# (\S+) -> content_size
LOG_PATTERN = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+)'

# Silver Schema Constants
TIMESTAMP_FORMAT = "dd/MMM/yyyy:HH:mm:ss Z"

RAW_LOG_PATH = "/Volumes/workspace/default/logs_volume/NASA_access_log_Jul95.gz"
BRONZE_PATH = "/Volumes/workspace/default/logs_volume/bronze"
SILVER_PATH = "/Volumes/workspace/default/logs_volume/silver"
GOLD_PATH = "/Volumes/workspace/default/logs_volume/gold"