package net.egemsoft.rrd.utils;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by cenk on 08/02/15.
 */
public class StringActions {
  final static Logger logger = Logger.getLogger(StringActions.class);


  public static List stringToList(String str) {
    return new ArrayList<String>(Arrays.asList(str.replace("[", "").replace("]", "").split(",")));
  }
}
