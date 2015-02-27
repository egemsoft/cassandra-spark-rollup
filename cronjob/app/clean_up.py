import subprocess

__author__ = 'cenk'

import sys
import datetime

from custom_logger import get_logger

import time

logger = get_logger()


def clean_up(args):
    keyspace = args[1]

    date_value = datetime.datetime.fromtimestamp(int(args[2]))

    start_time = datetime.datetime(date_value.year, date_value.month, date_value.day, date_value.hour, 00)
    start_time = int(start_time.strftime("%s"))

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
            p = subprocess.call(command, shell=True, stdout=subprocess.PIPE,
                                cwd="/home/sparkuser/cassandra-spark-rollup")
            logger.debug(p)

        except Exception, e:
            logger.error(e.message)
        start_time = start_time + (60 * 60)
        end_time = start_time + (60 * 60)
        time.sleep(300)

    date_value = datetime.datetime.fromtimestamp(int(args[2]))

    start_time = datetime.datetime(date_value.year, date_value.month, date_value.day, 00, 00)
    start_time = int(start_time.strftime("%s"))

    end_time = start_time + (60 * 60 * 24)
    while end_time < task_end:
        logger.debug("End Time: %s, Start Time: %s", end_time, start_time)
        command = "nohup /data/spark/bin/spark-submit --class net.egemsoft.rrd.Main  " \
                  "--master spark://ipam-ulus-db-2  target/cassandra-spark-rollup-1.0-driver.jar " \
                  " spMaster=spark://ipam-ulus-db-2:7077 casHost=ipam-ulus-db-1 " \
                  "casKeyspace=%s casTable=metric rollup=3600 start=%s end=%s destRollup=86400 ttl=94608000  &\n" % (
                      keyspace, start_time, end_time)
        logger.debug("Command: %s", command)
        try:
            p = subprocess.call(command, shell=True, stdout=subprocess.PIPE,
                                cwd="/home/sparkuser/cassandra-spark-rollup")
            logger.debug(p)

        except Exception, e:
            logger.error(e.message)
        start_time = start_time + (60 * 60 * 24)
        end_time = start_time + (60 * 60 * 24)
        time.sleep(1000)


clean_up(sys.argv)

####usage
## python clean_up.py ipam 1423785600