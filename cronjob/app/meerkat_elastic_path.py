#!/usr/bin/env python
import subprocess
import datetime

from meerkat_logger import get_logger
import sys

__author__ = 'cenk'

logger = get_logger()


def meerkat_elastic_path(args):
    keyspace = args[1]
    indexname = args[2]
    try:
        start_time = int(args[3])
        end_time = int(args[4])
    except:
        end_time = int(datetime.datetime.now().strftime("%s"))
        end_time = int(round(end_time / 100) * 100)
        start_time = end_time - (60 * 8)
    logger.debug("End Time: %s, Start Time: %s", end_time, start_time)
    command = "nohup /data/spark/bin/spark-submit --class net.egemsoft.rrd.elasticPaths.Main  " \
              "--master spark://ipam-ulus-db-2  target/meerkat-1.0-driver.jar " \
              " spMaster=spark://ipam-ulus-db-2:7077 casHost=ipam-ulus-db-1 indexname=%s " \
              " casKeyspace=%s casTable=metric start=%s end=%s &\n" % (
                  indexname, keyspace, start_time, end_time)
    logger.debug("Command: %s", command)
    try:
        p = subprocess.call(command, shell=True, stdout=subprocess.PIPE, cwd="/home/sparkuser/meerkat")
        logger.debug(p)
    except Exception, e:
        logger.error(e.message)


meerkat_elastic_path(sys.argv)
####usage
## python meerkat_elastic_path.py ipam ipam_cyanite_paths