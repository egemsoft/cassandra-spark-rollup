package net.egemsoft.rrd.models;

import java.io.Serializable;
import java.util.List;

/**
 * Created by cenk on 06/02/15.
 */
public class Metric implements Serializable {


  private String tenant;
  private int period;
  private int rollup;
  private String path;
  private Long time;
  private List<Double> data;

  public Metric(int rollup, String path, List<Double> data) {


    this.rollup = rollup;
    this.path = path;
    this.data = data;

    setTenant();
    setPeriod();
    setTime();
  }

  private void setTenant() {

  }

  private void setTime() {

  }

  private void setPeriod() {

  }

  public Metric(String tenant, int period, int rollup, String path, Long time, List<Double> data) {

    this.tenant = tenant;
    this.period = period;
    this.rollup = rollup;
    this.path = path;
    this.time = time;
    this.data = data;
  }

  public String getTenant() {
    return tenant;
  }

  public int getPeriod() {
    return period;
  }

  public int getRollup() {
    return rollup;
  }

  public String getPath() {
    return path;
  }

  public Long getTime() {
    return time;
  }

  public List<Double> getData() {
    return data;
  }
}
