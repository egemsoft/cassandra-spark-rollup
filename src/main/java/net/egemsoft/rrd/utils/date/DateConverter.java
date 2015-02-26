package net.egemsoft.rrd.utils.date;

import org.apache.log4j.Logger;

/**
 * Created by cenk on 08/02/15.
 */
abstract class DateConverter {
  final static Logger logger = Logger.getLogger(DateConverter.class);
  Integer constant;

  private int getConstant() {
    if (constant == null) {
      logger.error("You must set a constant value for date convert");
    }
    return constant;
  }

  public long convert(String dateString) {
    long value = Long.parseLong(dateString.replaceAll("\\D+", ""));
    return value * getConstant();
  }

}
