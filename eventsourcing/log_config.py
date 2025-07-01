import logging.handlers
import queue

# Thread-safe queue for log records
_log_queue: queue.SimpleQueue = queue.SimpleQueue()

# QueueHandler enqueues log records without blocking the calling thread
queue_handler = logging.handlers.QueueHandler(_log_queue)

# Console handler to actually emit the logs
console_handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
console_handler.setFormatter(formatter)

# QueueListener pulls from _log_queue and dispatches to console_handler
listener = logging.handlers.QueueListener(_log_queue, console_handler)
listener.start()

# Configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(queue_handler)
