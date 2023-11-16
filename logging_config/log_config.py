import logging

def setup_logging():
    logger_error = logging.getLogger('error_logger')
    logger_info = logging.getLogger('info_logger')

    # Check if handlers already exist
    if not logger_error.handlers:
        error_handler = logging.FileHandler('error_log.txt')
        error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger_error.addHandler(error_handler)
        logger_error.setLevel(logging.ERROR)

    if not logger_info.handlers:
        info_handler = logging.FileHandler('info_log.txt')
        info_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger_info.addHandler(info_handler)
        logger_info.setLevel(logging.INFO)