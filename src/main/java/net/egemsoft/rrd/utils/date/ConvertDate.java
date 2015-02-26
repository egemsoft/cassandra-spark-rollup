package net.egemsoft.rrd.utils.date;

import org.apache.log4j.Logger;

import java.io.Serializable;

/**
 * Created by cenk on 06/02/15.
 */
public class ConvertDate implements Serializable {
  final static Logger logger = Logger.getLogger(ConvertDate.class);

  public static long convertStringDateToSeconds(String value) {
    long result = 0;

    if (value.contains("s")) {
      result = new SecondToSecond().convert(value);
    } else if (value.contains("m")) {
      result = new MinutesToSecond().convert(value);
    } else if (value.contains("h")) {
      result = new HourToSecond().convert(value);
    } else if (value.contains("d")) {
      result = new DayToSecond().convert(value);
    } else if (value.contains("y")) {
      result = new YearToSecond().convert(value);
    } else {
      logger.error("You rollup config has errors");
    }
    return result;
  }

}
