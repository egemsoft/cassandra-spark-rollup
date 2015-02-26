package net.egemsoft.rrd.utils.date;

/**
 * Created by cenk on 08/02/15.
 */
public class DayToSecond extends DateConverter {

  public DayToSecond() {
    constant = 60 * 60 * 24;
  }
}
