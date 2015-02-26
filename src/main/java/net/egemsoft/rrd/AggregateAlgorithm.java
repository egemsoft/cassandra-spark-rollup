package net.egemsoft.rrd;

import com.datastax.spark.connector.japi.CassandraRow;
import net.egemsoft.rrd.utils.StringActions;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * Created by cenk on 05/02/15.
 */
public class AggregateAlgorithm implements Serializable {
  final static Logger logger = Logger.getLogger(AggregateAlgorithm.class);
  private JavaPairRDD<String, Double> results;

  protected void calculateAverage(JavaRDD<CassandraRow> javaRDD) {

    JavaPairRDD<String, Iterable<CassandraRow>> groupedData = javaRDD.groupBy(new Function<CassandraRow, String>() {
      @Override
      public String call(CassandraRow row) throws Exception {
        return row.getString("path");
      }
    });

    results = groupedData.mapToPair(new PairFunction<Tuple2<String, Iterable<CassandraRow>>, String, Double>() {
      @Override
      public Tuple2<String, Double> call(Tuple2<String, Iterable<CassandraRow>> t) throws Exception {
        Double total = 0.0;
        int count = 0;

        Iterable<CassandraRow> casandraRows = t._2();

        for (CassandraRow value : casandraRows) {
          String dataList = value.getString("data");
          List list = StringActions.stringToList(dataList);
          //logger.info(dataList);
          //TODO this get should be change to average
          total += Double.parseDouble((String) list.get(0));
          count++;
        }
        double mean = 0.0;
        try {
          mean = total / count;
        } catch (Exception e) {
          e.printStackTrace();
        }
        //String info = String.format("Total: %s, Mean: %s, Count: %s", total, mean, count);
        //logger.info(info);

        return new Tuple2<String, Double>(t._1(), mean);
      }

    });

  }

  public JavaPairRDD<String, Double> getResults() {
    return results;
  }

  protected void displayRollUp(JavaRDD<CassandraRow> javaRDD) {

    try {
      List<CassandraRow> rddResults = javaRDD.collect();
      for (CassandraRow row : rddResults) {
        String info = String.format("RollUp: %s", row.getInt("rollup"));
        //logger.info(info);

      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }


  protected void displayAverageAndPath(JavaRDD<CassandraRow> javaRDD) {
    calculateAverage(javaRDD);
    try {
      List<Tuple2<String, Double>> rddResults = results.collect();
      for (Tuple2<String, Double> row : rddResults) {
        String info = String.format("Path: %s, Double: %s", row._1(), row._2());
        logger.info(info);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


}
