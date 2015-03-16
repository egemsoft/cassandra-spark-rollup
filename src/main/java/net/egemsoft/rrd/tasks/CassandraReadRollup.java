package net.egemsoft.rrd.tasks;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import net.egemsoft.rrd.SparkConfig;
import org.apache.spark.SparkConf;

/**
 * Created by cenk on 27/02/15.
 */
public class CassandraReadRollup {
  public static void main(String[] args) {
    SparkConf sparkConfig = new SparkConfig("spark://ipam-ulus-db-2:7077", "ipam-ulus-db-2", "Tasks").getSparkConf();
    int counter = 0;
    System.out.println("Start");
    CassandraConnector cassandraConnector = CassandraConnector.apply(sparkConfig);
    try (Session session = cassandraConnector.openSession()) {

      ResultSetFuture results = session.executeAsync("SELECT path,rollup from ipam.metric");

      for (Row result : results.get()) {
        System.out.println(counter);
        if (result.getInt("rollup") == 86400) {
          String path = result.getString("path");
          System.out.println(result);
          String deleteCql = String.format("delete from ipam.metric where tenant= '' and rollup = 86400 and period = 1095 and path = '%s' ", path);
          System.out.println(deleteCql);
          session.executeAsync(deleteCql);
        }
        counter++;
      }

    } catch (Exception e) {
      e.printStackTrace();
    }


  }
}
