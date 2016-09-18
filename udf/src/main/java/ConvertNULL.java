package com.ingest.project;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author edwin permana
 * class ConvertNULL values provides UDF function to sanitize NULL null to "" as in database
 * case NULL
 * case null
 * case space
 */
public final class ConvertNULL extends UDF {

  // public static void main(String[] args) {
  //   ConvertNULL sc = new ConvertNULL();
  //   System.out.println(sc.evaluate(new Text("  ")));
  //   System.out.println(sc.evaluate(new Text("null")));
  //   System.out.println(sc.evaluate(new Text("NULL")));
  //   System.out.println(sc.evaluate(new Text("bukan null")));
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

      if (s.equalsIgnoreCase("null") || s.length() == 0) {
        retval = new Text("");
      }
      else {
        retval = new Text(s);
      }

    }

    return retval;
  }

}
