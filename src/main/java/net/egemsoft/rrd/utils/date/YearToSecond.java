package net.egemsoft.rrd.utils.date;

/**
 * Created by cenk on 08/02/15.
 */
public class YearToSecond extends DateConverter {

  public YearToSecond() {
    constant = 60 * 60 * 12 * 365;
  }
}
