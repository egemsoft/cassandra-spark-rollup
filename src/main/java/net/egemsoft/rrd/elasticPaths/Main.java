package net.egemsoft.rrd.elasticPaths;

import net.egemsoft.rrd.utils.InputParams;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by cenk on 09/02/15.
 */
public class Main {
  final static Logger logger = Logger.getLogger(Main.class);

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    logger.info("Start");
    Map map = InputParams.paramsToMap(args);

    ElasticPath elasticPath = new ElasticPath((String) map.get("indexname"));
    try {
      elasticPath.setMapping(elasticPath.checkIndexExists());
    } catch (IOException e) {
      e.printStackTrace();
    }

    Action action = new ElasticAction(elasticPath);
    Parser parser = new PathParser();

    Cassandra cassandra = new Cassandra(map);
    cassandra.distinct(parser, action);

    logger.info("Finish");
  }

}
