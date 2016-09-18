package com.ingest.project;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author edwin permana
 * class ConvertSign provides UDF function to convert back - sign to the front
 * will call NULL cleansing as well
 */
public final class ConvertSign extends UDF {

  // public static void main(String[] args) {
  //   ConvertSign sc = new ConvertSign();
  //   System.out.println(sc.evaluate(new Text("2134.00-")));
  //   System.out.println(sc.evaluate(new Text("2134.00")));
  // }

  /**
   * evaluate
   */
  public Text evaluate(final Text t) {

    Text retval = null;

    if (t == null) {
      retval = new Text("");
    }
    else {
      String s = t.toString().trim();

      if (s.endsWith("-")) {
        s = "-" + s.substring(0, s.length()-1);
      }

      retval = new Text(s);
    }

    return retval;
  }

}

