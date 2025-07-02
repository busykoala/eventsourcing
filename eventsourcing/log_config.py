import json
import logging
import logging.handlers
import queue

# Thread-safe queue for log records
_log_queue: queue.SimpleQueue = queue.SimpleQueue()

# QueueHandler enqueues log records without blocking
queue_handler = logging.handlers.QueueHandler(_log_queue)

# Console handler to actually emit the logs
console_handler = logging.StreamHandler()

# Start a listener for the queue
listener = logging.handlers.QueueListener(_log_queue, console_handler)
listener.start()

# Use a dedicated logger name so we don’t clobber the root or uvicorn
logger = logging.getLogger("eventsourcing")
logger.setLevel(logging.DEBUG)
logger.addHandler(queue_handler)


def configure_logging(json_logging: bool = False) -> None:
    """
    Call this at application startup to toggle JSON vs text logging.
    """
    if json_logging:

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
            logging.Formatter(
                "%(asctime)s %(levelname)s [%(name)s] %(message)s"
            )
        )
