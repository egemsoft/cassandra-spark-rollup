#!/usr/bin/env python
import subprocess
import datetime

from meerkat_logger import get_logger

import sys
__author__ = 'cenk'

logger = get_logger()


def hourly_rollup(args):
    keyspace = args[1]
    end_time = int(datetime.datetime.now().strftime("%s"))
    end_time = int(round(end_time / 100) * 100)
    start_time = end_time - (60 * 60)
    logger.debug("End Time: %s, Start Time: %s", end_time, start_time)
    command = "nohup /data/spark/bin/spark-submit --class net.egemsoft.rrd.Main  " \
              "--master spark://ipam-ulus-db-2  target/cassandra-spark-rollup-1.0-driver.jar " \
              " spMaster=spark://ipam-ulus-db-2:7077 casHost=ipam-ulus-db-1 " \
              "casKeyspace=%s casTable=metric rollup=300 start=%s end=%s destRollup=3600 ttl=7776000 &\n" % (
                  keyspace,start_time, end_time)
    logger.debug("Command: %s", command)
    try:
        p = subprocess.call(command, shell=True, stdout=subprocess.PIPE, cwd="/home/sparkuser/cassandra-spark-rollup")
        logger.debug(p)
    except Exception, e:
        logger.error(e.message)


hourly_rollup(sys.argv)

## python meerkat_elastic_path.py ipam
