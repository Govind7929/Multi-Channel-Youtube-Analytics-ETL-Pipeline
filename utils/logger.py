import logging
import os from logging.handlers import RotatingFileHandler
from venv import logger

def get_logger(name: str, log_dir: str = "logs") -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger
    
    logger.setlevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler = RotatingFileHandler(
        os.path.join(log_dir, "pipeline.log"),
        maxBytes=2_000_000,
        backupCount=5
    )

    file_handler.setFormatter(formatter)

    stream_handler = logging.Streamhandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addhandler(stream_handler)

    return logger