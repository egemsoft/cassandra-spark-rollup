package net.egemsoft.rrd.utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by cenk on 06/02/15.
 */
public class InputParams implements Serializable {

  public static Map<String, String> paramsToMap(String[] args) {

    String[] kvPairs = new String[args.length];
    if (args.length > 0) {
      for (int i = 0; i < args.length; i++) {
        kvPairs[i] = args[i].replaceAll("^\"|\"$", "");
      }
    }

    Map<String, String> map = new HashMap<String, String>();

    for (String entry : kvPairs) {
      String[] keyValue = entry.split("=");
      map.put(keyValue[0].toLowerCase(), keyValue[1]);
    }
    return map;
  }
}
