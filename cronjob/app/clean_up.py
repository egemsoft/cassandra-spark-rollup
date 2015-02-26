import subprocess

__author__ = 'cenk'

import sys
import datetime

from meerkat_logger import get_logger

import time

logger = get_logger()


def clean_up(args):

    keyspace = args[1]
    start_time = int(args[2])
    end_time = start_time + (60 * 60)
    task_end = int(datetime.datetime.now().strftime("%s"))
    while end_time < task_end:
        logger.debug("End Time: %s, Start Time: %s", end_time, start_time)
        command = "nohup /data/spark/bin/spark-submit --class net.egemsoft.rrd.Main  " \
                  "--master spark://ipam-ulus-db-2  target/cassandra-spark-rollup-1.0-driver.jar " \
                  " spMaster=spark://ipam-ulus-db-2:7077 casHost=ipam-ulus-db-1 " \
                  "casKeyspace=%s casTable=metric rollup=300 start=%s end=%s destRollup=3600 ttl=7776000 &\n" % (
                      keyspace, start_time, end_time)
        logger.debug("Command: %s", command)
        try:
            p = subprocess.call(command, shell=True, stdout=subprocess.PIPE, cwd="/home/sparkuser/cassandra-spark-rollup")
            logger.debug(p)

        except Exception, e:
            pass
            logger.error(e.message)
        start_time = start_time + (60 * 60)
        end_time = start_time + (60 * 60)
        time.sleep(300)


# meerkat_older_timer("")
clean_up(sys.argv)

####usage
## python meerkat_elastic_path.py ipam 1423785600