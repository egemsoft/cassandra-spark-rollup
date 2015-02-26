package net.egemsoft.rrd.utils.cassandra;

import org.apache.log4j.Logger;
import scala.Tuple2;

/**
 * Created by cenk on 08/02/15.
 */
public class Cql {

  final static Logger logger = Logger.getLogger(Cql.class);

  public static String cassandraRowToCql(Tuple2<String, Double> cassandraRow, String tableName, int period, int rollup, Long time, int ttl) {
    String cql = "";
    String path = cassandraRow._1();
    Double average = cassandraRow._2();
    try {
      //String cqlInsertTemplate = "insert into %s (tenant, period, rollup, path, time, data) values (%s) using ttl %s";
      String cqlUpdateTemplate = "UPDATE %s USING TTL %s SET data = %s " +
          "WHERE tenant = '' AND rollup = %s AND period = %s AND path = %s AND time = %s;";
      String values = "''," + period + "," + rollup + ",'" + path + "'," + time + ",[" + average + "]";
      cql = String.format(cqlUpdateTemplate, tableName, ttl, "[" + average + "]", rollup, period, path, time);
      logger.info(cql);
    } catch (Exception e) {
      e.printStackTrace();
    }

    return cql;
  }

  public static String insertCql(Tuple2<String, Double> cassandraRow, String tableName, int period, int rollup, Long time, int ttl) {
    String cql = "";
    String path = cassandraRow._1();
    Double average = cassandraRow._2();
    try {
      String cqlInsertTemplate = "insert into %s (tenant, period, rollup, path, time, data) values (%s) using ttl %s";
      String values = "''," + period + "," + rollup + ",'" + path + "'," + time + ",[" + average + "]";
      cql = String.format(cqlInsertTemplate, tableName, values, ttl);
      logger.info(path);
    } catch (Exception e) {
      e.printStackTrace();
    }

    return cql;
  }


}
