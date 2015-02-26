#!/usr/bin/env python
import logging



def get_logger():
    logging.basicConfig(filename='/home/sparkuser/logs/timer.log', level=logging.DEBUG,)
    logger = logging.getLogger(__name__)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger
