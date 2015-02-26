package net.egemsoft.rrd.elasticPaths;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by cenk on 09/02/15.
 */
public class DisplayData implements Action {
  final static Logger logger = Logger.getLogger(DisplayData.class);


  @Override
  public void processArrayList(ArrayList<Map> paths) {
    logger.info(paths.toString());
  }

  @Override
  public void processList(List list) {

  }
}
