import json
import logging.handlers
import queue

from eventsourcing.config import ESConfig

# Thread-safe queue for log records
_log_queue: queue.SimpleQueue = queue.SimpleQueue()

# QueueHandler enqueues log records without blocking the calling thread
queue_handler = logging.handlers.QueueHandler(_log_queue)

# Console handler to actually emit the logs
console_handler = logging.StreamHandler()

# Determine JSON vs plain-text based on ESConfig.json_logging
_cfg = ESConfig()  # uses defaults; json_logging=False unless overridden

if _cfg.json_logging:

    class JSONFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            payload = {
                "time": self.formatTime(record),
                "level": record.levelname,
                "name": record.name,
                "message": record.getMessage(),
            }
            return json.dumps(payload)

    console_handler.setFormatter(JSONFormatter())
else:
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    )

# QueueListener pulls from _log_queue and dispatches to console_handler
listener = logging.handlers.QueueListener(_log_queue, console_handler)
listener.start()

# Configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(queue_handler)
