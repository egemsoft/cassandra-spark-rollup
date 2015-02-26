package net.egemsoft.rrd.tasks;

import net.egemsoft.rrd.SparkConfig;
import net.egemsoft.rrd.models.Metric;
import org.apache.spark.api.java.JavaRDD;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

/**
 * Created by cenk on 11/02/15.
 */
public class CassandraDataCreator {


  public void createData() {
    SecureRandom random = new SecureRandom();

    int counter = 0;
    ArrayList<Metric> metrics = new ArrayList<Metric>();
    while (counter < 100000) {

      long time = Math.round(new Date().getTime() / 1000);
      List<Double> data = new ArrayList<Double>();
      data.add(1.0 * counter);
      data.add(2.0 * counter);
      data.add(3.0 * counter);
      String path = new BigInteger(130, random).toString(32) + '.' +
          new BigInteger(130, random).toString(32) + '.' + new BigInteger(130, random).toString(32) + '.' +
          new BigInteger(130, random).toString(32);
      metrics.add(new Metric("", 1000, 1000, path, time, data));
      counter++;

    }

    SparkConfig sparkConfig = new SparkConfig("local", "ipam-ulus-db-2", "Tasks");
    JavaRDD<Metric> saveRdd = sparkConfig.getJavaSparkContext().parallelize(metrics);

    javaFunctions(saveRdd).writerBuilder("test", "metric", mapToRow(Metric.class)).saveToCassandra();

  }

}
