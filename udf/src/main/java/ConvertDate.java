package com.ingest.project;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author edwin permana
 * class ConvertDate provides UDF function to convert Date to format YYYY-MM-DD
 * case YYYYMMDD 
 * case YYYYMMD
 * will call NULL cleansing as well
 */
public final class ConvertDate extends UDF {

  // public static void main(String[] args) {
  //   ConvertDate sc = new ConvertDate();
  //   System.out.println(sc.evaluate(new Text("20121205")));
  //   System.out.println(sc.evaluate(new Text("2012011")));
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

      if (s.length()==8) {
        s = s.substring(0, 4) + "-" + s.substring(4, 6) + "-" + s.substring(6, 8) + " 00:00:00";
      }
      else if (s.length()==7) {
        s = s.substring(0, 4) + "-" + s.substring(4, 6) + "-" + "0" + s.substring(6, 7) + " 00:00:00";
      }
      else if (s.equals("00000000")) {
        s = "";
      }

      retval = new Text(s);
    }

    return retval;
  }

}

