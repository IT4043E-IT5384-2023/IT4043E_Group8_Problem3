import sys
import logging
import os

# logs in utils/../logs/, aka. project root dir
LOG_OUTDIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../logs")

formatter = logging.Formatter(
    '%(asctime)s %(levelname)s %(name)s: %(message)s',
    datefmt='%Y/%m/%d %H:%M:%S'
)

def logger(name):
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y/%m/%d %H:%M:%S',
        level=logging.INFO
    )
    
    logger = logging.getLogger(name)

    handler_file = logging.FileHandler(f"{LOG_OUTDIR}/{name}.log", 'a')
    handler_file.setFormatter(formatter)
    logger.addHandler(handler_file)

    return logger