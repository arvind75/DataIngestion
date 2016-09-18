package genddl

/**
 * This Class takes a csv file of input DDL and converted it to Hive schema DDL
 * IF NUMERIC and Decimal specified the length then Converted Decimal will specify Decimal (10,0) 
 * If on Length Date specified YYYY-MM-DD then Mapped to Date instead of string 
 */
class Convertddl {

  // main
  static main(args) {

    // Five Args
//    String file = args[0];
    String table = args[0]; //table name
    String interface_nm = args[1]; //hive db
    String hdfspath = args[3]; //hdfs staging area
    String fileformat = args[2]; //file format
    String header_file=args[4]; //header file name with path
    String partitions = args[5];  // Partitions
    String incr_flag = args[6];  // Refresh Type <incr/full>
    String src_schema = args[7];  // Source schema name
 

    def env = System.getenv()
    def func_home = env['FUNC_HOME']
    def out_home = env['OUT_HOME']
    def headers_home = env['HEADER_HOME']

// Lookin for folder and if does not exist create one in out_home
   def folder = new File( out_home + "/" + src_schema + "." + table );
   String file= headers_home + "/" + header_file

// If it doesn't exist
  if( !folder.exists() ) {
    if(folder.mkdir()) { println "Directory " + out_home + "/" + src_schema + "." + table + " created successfully" }
  }

    String apl = '';
    String storage = "";

    def timePeriodsScript = new GroovyScriptEngine(func_home).with { loadScriptByName('TimePeriods.groovy'); }
    Convertddl.getClass().metaClass.mixin timePeriodsScript;

    if (partitions=='none') {
      // no partitions //
    }
    else if (partitions in validYears()) {
      storage += "PARTITIONED BY (year string) \n"
        apl += addPartitions(table, partitions, "year") + "\n"
    }
    else if (partitions =~ /^20[01][0123456789]Q[1234]$/ ||
        partitions =~ /^20[01][0123456789]Q[1234]-20[01][0123456789]Q[1234]$/) {
      storage += "PARTITIONED BY (quarter string) \n"
        apl += addPartitions(table, partitions, "quarter") + "\n"
    }
    else if (partitions =~ /^20[01][0123456789](01|02|03|04|05|06|07|08|09|10|11|12)$/ ||
        partitions =~ /^20[01][0123456789](01|02|03|04|05|06|07|08|09|10|11|12)-20[01][0123456789](01|02|03|04|05|06|07|08|09|10|11|12)$/) {
      storage += "PARTITIONED BY (month string) \n"
        apl += addPartitions(table, partitions, "month") + "\n"
    }
    else if (partitions == "month" || partitions == "year"||partitions == "quarter") {  /*Added by Arvind*/
      storage +="PARTITIONED BY (" + partitions + " string) \n"

    }
    else {
      throw new RuntimeException("Incorrect pattern for partitions")
    }

    def tblprop = [];

    storage += "STORED AS " + fileformat + "\n"

/*
#    if (format.equals("avro")) {
#     storage += "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' \n" +
#       "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' \n" +
#       "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'";
#   }
#   else if (format.startsWith("orc")) {
#     def parts = format.split("__");
#     if (parts.length != 3) {
#       throw new RuntimeException("Incorrect pattern for orc format; need something like 'orc__id__3'")
#     }
#     storage += "CLUSTERED BY (" + parts[1] + ") INTO " + parts[2] + " BUCKETS \n";
#     storage += "STORED AS ORC \n";
#     tblprop << "'transactional'='true'";
#   }
#   else {
#     storage += "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' \n" +
#       "STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' \n" +
#       "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' \n";
#   }

#    String sets = "";
#
#    if (format.startsWith("orc")) {
#      sets += "set hive.support.concurrency=true; \n";
#      sets += "set hive.enforce.bucketing=true; \n";
#      sets += "set hive.exec.dynamic.partition.mode=nonstrict; \n";
#      sets += "set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; \n";
#      sets += "set hive.compactor.initiator.on=true; \n";
#    }
*/

     String ddl = "\nuse " + interface_nm + ";\n\n"
     if ( incr_flag == "incr" ) {ddl+="create external table if not exists " + table + "_incr" + " (" + "\n";}
     else {ddl+="create external table if not exists " + table + " (" + "\n";}

    def script = new GroovyScriptEngine(func_home).with {
      loadScriptByName('DataTypeMappings.groovy');
    } 

    Convertddl.getClass().metaClass.mixin script;

    use(Convertddl.class) {
      File infile = new File(file)
        String conv;
      infile.parseCSV { index,field ->
        if (index > 1) {
          String commentField = field[2];
          if (commentField==null) commentField = "";
          commentField = commentField.replaceAll(/'/,"\\\\'").replaceAll(/;/,"\\\\;");
          if (field[1].trim().equalsIgnoreCase("DATE") || field[1].equalsIgnoreCase("TIMESTAMP")|| field[1].equalsIgnoreCase("DATS")) {
            ddl += sprintf("  %-33s%-33s%33s\n", field[0], "timestamp", " COMMENT '" + commentField + "',")
          }
          else if (field[1].trim().equalsIgnoreCase("NUMBER")) {
            ddl += sprintf("  %-33s%-33s%33s\n", field[0], "double", " COMMENT '" + commentField + "',")
          }
          else if (field[1].trim().equalsIgnoreCase("NUMERIC") || field[1].trim().equalsIgnoreCase("DECIMAL")) {
            String precision = field[3]
              if (precision==null || !precision.isNumber()) precision = "10"
                String scale = field[4]
                  if (scale==null || !scale.isNumber()) scale = "0"
                    ddl += sprintf("  %-33s%-33s%33s\n", field[0], dataTypeMappings().get("NUMERIC") + "(" + precision + ","  + scale + ")", " COMMENT '" + commentField + "',")
          }
          else {
            ddl += sprintf("  %-33s%-33s%33s\n",
                field[0].trim(), dataTypeMappings().get(field[1].trim().toUpperCase()),
                " COMMENT '" + commentField + "',")
          }
        }
      }
    }

    tblprop << "'parquet.compression'='SNAPPY', 'serialization.null.format'='','escape.delim'='\\\\','field.delim'='|'"
//    tblprop << "'serialization.null.format'=''";

    String sql = ddl;
    ddl = sql.substring(0,sql.length()-2);
    ddl += "\n)\n" + storage + "LOCATION '" + hdfspath + "'\n" +
    tblprop.toString().replace('[','TBLPROPERTIES(').replace(']',')') + ";\n\n";

    ddl += apl;

    new File( folder.getAbsolutePath()+ "/"  + table + ".sql").write(ddl);
//    new File(  table + ".sql").write(ddl);
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

  // addPartitions
  static def addPartitions(table, partitions, frequency) {

    def apl = ""

      def list = []

      if (frequency=='month') {                              // monthly or yearly
        def l = partitions.split('-')
          if (l.length==1) {
            partitions = partitions + "-" + partitions
              l = partitions.split('-')
          }
        def r = l[0].toInteger()..l[1].toInteger()
          r.each {
            if (frequency=='month') {
              def s = it.toString().substring(4)
                def n = it.toString().substring(4).toInteger()
                if (s != '00' && n <= 12) list << it
            }
            else {
              list << it
            }
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
                list << it.toString().
                  replaceFirst(/01$/, "Q1").
                  replaceFirst(/02$/, "Q2").
                  replaceFirst(/03$/, "Q3").
                  replaceFirst(/04$/, "Q4")
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
            list << "FY${it}"
          }
      }
      else {
        throw new RuntimeException("Unknown frequency")
      }

    list.each {
      apl += "alter table " + table + " add if not exists partition (" + frequency + "='" + it + "');\n"
    }

    apl
  }

}

