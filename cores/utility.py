import logging
import os

def init_file_log(logger_name):
    logger, formatter = init_log(logger_name, logging.WARNING)
    if not has_file_handler(logger):
        file_handler = logging.FileHandler(os.path.join('Logs',logger_name))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger

def init_console_log(logger_name):
    logger, formatter = init_log(logger_name, logging.DEBUG)
    if not has_stream_handler(logger):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    return logger

def init_log(logger_name, log_level):
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    formatter = logging.Formatter('[%(asctime)s] - %(levelname)s -  %(message)s')
    return logger, formatter

def has_file_handler(logger):
    hasHandler = False
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            hasHandler = True
            break
    return hasHandler

def has_stream_handler(logger):
    hasHandler = False
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            hasHandler = True
            break
    return hasHandler
