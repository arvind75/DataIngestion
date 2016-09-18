package genddl

/**
 * This Class takes a csv file of input DDL and converted it to Hive schema DDL
 * IF NUMERIC and Decimal specified the length then Converted Decimal will specify Decimal (10,0) 
 * If on Length Date specified YYYY-MM-DD then Mapped to Date instead of string 
 * Change History
 * Arvind - Added change for dynamic partitioning 
 */
class TransformRAWData {

  // main
  static main(args) {

    // Three Args
    String header_file = args[0];
    String interface_nm = args[1];
    String table = args[2];
    String partitions = args[3];
    String incr_flag = args[4];
    String src_schema = args[5];

    def env = System.getenv()
    def func_home = env['FUNC_HOME']
    def out_home = env['OUT_HOME']
    def headers_home = env['HEADER_HOME']

    def folder = new File( out_home + "/" + src_schema + "." + table );
    String file= headers_home + "/" + header_file

    String dedup_view, dml2, new_table, old_table,  incr_table, raw_table;

    def partitionsList = [];

    def timePeriodsScript = new GroovyScriptEngine(func_home).with { loadScriptByName('TimePeriods.groovy'); }
    TransformRAWData.getClass().metaClass.mixin timePeriodsScript;

    if (partitions=='none') {
      // no partitions //
      partitionsList << 'dummy';
    }
    else if (partitions in validYears()) {
      partitionsList = listPartitions(table, partitions, "year");
    }
    else if (partitions =~ /^20[01][0123456789]Q[1234]$/ ||
        partitions =~ /^20[01][0123456789]Q[1234]-20[01][0123456789]Q[1234]$/) {
      partitionsList = listPartitions(table, partitions, "quarter");
    }
    else if (partitions =~ /^20[01][0123456789](01|02|03|04|05|06|07|08|09|10|11|12)$/ ||
        partitions =~ /^20[01][0123456789](01|02|03|04|05|06|07|08|09|10|11|12)-20[01][0123456789](01|02|03|04|05|06|07|08|09|10|11|12)$/) {
      partitionsList = listPartitions(table, partitions, "month");
    }
    else if (partitions == "month" || partitions == "year"||partitions == "quarter") {  /*Added by Arvind*/
      partitionsList << partitions;
    }
    else {
      throw new RuntimeException("Incorrect pattern for partitions")
    }

    String dml = "\nuse " + interface_nm + ";\n" +
      // "\nadd jar hivecleaningudf-0.0.1-SNAPSHOT.jar;\n" +
      "\ncreate temporary function ConvertDate as 'com.ingest.project.ConvertDate';" +
      "\ncreate temporary function ConvertSign as 'com.ingest.project.ConvertSign';" +
      "\ncreate temporary function ConvertNULL as 'com.ingest.project.ConvertNULL';\n" +
      "\nSET hive.exec.parallel = true;" +
      "\nSET hive.exec.compress.output = true;" +
      "\nSET mapred.output.compression.type = BLOCK;" +
      "\nSET mapred.output.compression.codec = org.apache.hadoop.io.compress.SnappyCodec;;" +
      "\nSET hive.exec.dynamic.partition=true;" +   /*Added by Arvind +next line*/
      "\nSET hive.exec.dynamic.partition.mode=nonstrict;\n";

    if ( incr_flag == "incr" ) {
      dedup_view = table + "_view";
      incr_table=table + "_incr";
      new_table = table + "_new"
      old_table = table + "_old"
      raw_table = incr_table + "_raw"
    }
    else
    {
      raw_table= table + "_raw"
    }

    new File(folder.getAbsolutePath()+ "/"  + table + "_cleanup" + ".sql").write(dml);

    def script = new GroovyScriptEngine(func_home).with {
      loadScriptByName('DataTypeMappings.groovy');
    } 

    TransformRAWData.getClass().metaClass.mixin script;

    String[] dateTypes = [ "DATE", "DATS", "TIMESTAMP" ]

      // println partitionsList;

      // ------------
      // iterate ...
      // ------------
      partitionsList.each {

        if (it=='dummy' && incr_flag != "incr") {
          dml = "\ninsert overwrite table " + table + " select " + "\n";
        }
        else if (it=='dummy' && incr_flag == "incr") {
          dml = "\ninsert overwrite table " + incr_table + " select " + "\n";
        }
        else if ((it =='year' || it == 'quarter' || it == 'month')&& incr_flag == "incr") {
          dml = "\ninsert overwrite table " + incr_table + " partition (" + it + ") select " + "\n" ;
          dml2 = "\nuse " + interface_nm + ";\n" +
                 "\nSET hive.exec.parallel = true;" +
                 "\nSET hive.exec.compress.output = true;" +
                 "\nSET mapred.output.compression.type = BLOCK;" +
                 "\nSET mapred.output.compression.codec = org.apache.hadoop.io.compress.SnappyCodec;;" +
                 "\nSET hive.exec.dynamic.partition=true;" +   /*Added by Arvind +next line*/
                 "\nSET hive.exec.dynamic.partition.mode=nonstrict;\n" +
                 "\ninsert overwrite table " + new_table + " partition (" + it + ") select * from " + dedup_view + ";\n" +
                 "\nanalyze table " + new_table + " partition (" + it + ") compute statistics" + ";\n" +
                 "\nalter table " + table + " rename to " + old_table + ";\n" +
                 "\nalter table " + new_table + " rename to " + table + ";\n" +
                 "\nalter table " + old_table + " rename to " + new_table + ";\n";
          new File(folder.getAbsolutePath()+ "/"  + table + "_dedup" + ".sql").write(dml2);
        }
        else {
          dml = "\ninsert into table " + table + " partition (" + it + ") select " + "\n";
        }
        use(TransformRAWData.class) {
          File infile = new File(file)
            infile.parseCSV { index,field ->
              if (index > 1) {
                String field0 = field[0].trim()
                  String dataType = dataTypeMappings().get(field[1].trim().toUpperCase())
                  if (dataType.toUpperCase() in dateTypes) {
                    dml += "  ConvertDate(" + field0 + "),\n";
                  }
                  else if (dataType.equalsIgnoreCase("Decimal")) {
                    dml += "  Cast(Cast(ConvertSign("  + field0 + ") as double) as decimal(17,2)),\n"
                  }
                  else  {
                    dml += "  ConvertNULL(" + field0 + "),\n"
                  }
              }
            }
        }

        String sql = dml;
        dml = sql.substring(0,sql.length()-2);
        if (it == 'year' || it == 'quarter' || it == 'month') {  /*Added by Arvind for Incrementals*/
          dml +="\n, ConvertNULL(" + it + ") "
        }
        dml += "\nfrom " + interface_nm + "_raw." + raw_table + "\n";

        if (it=='dummy' || it == 'year' || it == 'quarter' || it == 'month') {  /*Modified by Arvind*/
          dml += ";\n";
        }
        else {
          dml += "where " + it + ";\n";
        }

        if (incr_flag == 'incr') {dml += "\nanalyze table ${incr_table} "} else {dml += "\nanalyze table ${table} "};
        if (it != 'dummy') dml += "partition (${it}) ";
        dml += "compute statistics;\n";

        new File(folder.getAbsolutePath()+ "/"  + table + "_cleanup" + ".sql").append(dml);
      }
  }

  // parseCSV
  static def parseCSV(file,closure) {
    def lineCount = 0
      file.eachLine() { line ->
        def field = line.tokenize(",")
          lineCount++
          closure(lineCount,field)
      }
  }

  // listPartitions
  static def listPartitions(table, partitions, frequency) {

    def list = []

      if (frequency=='month') {                              // monthly
        def l = partitions.split('-')
          if (l.length==1) {
            partitions = partitions + "-" + partitions
              l = partitions.split('-')
          }
        def r = l[0].toInteger()..l[1].toInteger()
          r.each {
            def s = it.toString().substring(4)
              def n = it.toString().substring(4).toInteger()
              if (s != '00' && n <= 12) list << "month='" + it + "'";
          }
      }
      else if (frequency=='quarter') {                       // quarterly
        def l = partitions.split('-')
          if (l.length==1) {
            partitions = partitions + "-" + partitions
              l = partitions.split('-')
          }
        def r = l[0].replace('Q','0').toInteger()..l[1].replace('Q','0').toInteger()
          r.each {
            def s = it.toString().substring(4)
              def n = it.toString().substring(4).toInteger()
              if (s != '00' && n <= 4) {
                list << "quarter='" + it.toString().
                  replaceFirst(/01$/, "Q1").
                  replaceFirst(/02$/, "Q2").
                  replaceFirst(/03$/, "Q3").
                  replaceFirst(/04$/, "Q4") + "'";
              }
          }
      }
      else if (frequency=='year') {                          // yearly
        def l = partitions.split('-')
          if (l.length==1) {
            partitions = partitions + "-" + partitions
              l = partitions.split('-')
          }
        def r = l[0].replace('FY','').toInteger()..l[1].replace('FY','').toInteger()
          r.each {
            list << "year='FY${it}'"
          }
      }
      else {
        throw new RuntimeException("Unknown frequency")
      }

    list
  }

}
