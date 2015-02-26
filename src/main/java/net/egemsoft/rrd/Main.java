package net.egemsoft.rrd;

import net.egemsoft.rrd.utils.InputParams;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * Created by cenk on 04/02/15.
 */
public class Main {
  final static Logger logger = Logger.getLogger(Main.class);

  public static void main(String args[]) {
    Map argsMap = InputParams.paramsToMap(args);

    logger.info("Spawning");
    logger.info(argsMap.entrySet());
    SparkRollUp program = new SparkRollUp(argsMap);
    program.run();
    logger.info("Job Finishes");

  }
}
