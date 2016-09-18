package genddl

/**
 * This Class takes a csv file of input DDL and converted it to Hive schema DDL
 * IF NUMERIC and Decimal specified the length then Converted Decimal will specify Decimal (10,0) 
 * If on Length Date specified YYYY-MM-DD then Mapped to Date instead of string 
 */
class ConvertRawData {

  // main
  static main(args) {

    // Four Args
    String table = args[0]        // Header (CSV) File
    String interface_nm = args[1]    // Hive Database; Interface Name
    String hdfspath = args[2]    // HDFS Path for Raw Data
    String header_file=args[3]   // Header File
    String partitions = args[4]  // Partitions 
    String fileformat=args[5]   //fileformat
    String incr_flag = args[6]  // refresh type <incr/full>
    String src_schema = args[7]  // Source Schema

//    String table = file.tokenize('.')[0];

    def env = System.getenv()
    def func_home = env['FUNC_HOME']
    def headers_home = env['HEADER_HOME']
    def out_home = env['OUT_HOME']
    def raw_landing_path = env['RAW_LANDING_PATH']

// Lookin for folder and if does not exist create one in out_home
   def folder = new File( out_home + "/" + src_schema + "." + table );

// If it doesn't exist
  if( !folder.exists() ) {
    if(folder.mkdir()) { println "Directory " + out_home + "/" + src_schema + "." + table + " created successfully" }
  }

//    String table = file.tokenize('.')[0];
    String file= headers_home + "/" + header_file



      if (!interface_nm.endsWith("_raw")) {
        interface_nm += "_raw"
      }


    String ddl = "\nuse " + interface_nm + ";\n\n"
    if ( incr_flag == "incr" ) {ddl+="create external table if not exists " + table + "_incr" + "_raw" + " (" + "\n";}
    else {ddl+="create external table if not exists " + table + "_raw" + " (" + "\n";}

    use(ConvertRawData.class) {
      File infile = new File(file)
        infile.parseCSV { index,field ->
          if (index > 1) { 
            ddl += sprintf("  %-33sString,\n", field[0]);
          }
        }
    }

    String sql = ddl
      ddl = sql.substring(0,sql.length()-2)
      ddl = sql.substring(0,sql.length()-2)
      if ((partitions == 'year' || partitions == 'quarter' || partitions == 'month') && (incr_flag == "append" || incr_flag == "incr" )) {  /*Added by Arvind for Incrementals*/
       ddl +=  sprintf("\n, %-33sString\n", partitions);
      }

      ddl += "\n) \n"

      def apl = ""

      def timePeriodsScript = new GroovyScriptEngine(func_home).with { loadScriptByName('TimePeriods.groovy'); }
    ConvertRawData.getClass().metaClass.mixin timePeriodsScript;

    // println "=> Valid Years :: ${validYears()}"

    if (partitions=='none' || incr_flag == "incr" ) {
      // no partitions //
    }
    else if (partitions in validYears()) {
      ddl += "PARTITIONED BY (year string) \n"
        apl += addPartitions(table, partitions, "year") + "\n"
    }
    else if (partitions =~ /^20[01][0123456789]Q[1234]$/ ||
        partitions =~ /^20[01][0123456789]Q[1234]-20[01][0123456789]Q[1234]$/) {
      ddl += "PARTITIONED BY (quarter string) \n"
        apl += addPartitions(table, partitions, "quarter") + "\n"
    }
    else if (partitions =~ /^20[01][0123456789](01|02|03|04|05|06|07|08|09|10|11|12)$/ ||
        partitions =~ /^20[01][0123456789](01|02|03|04|05|06|07|08|09|10|11|12)-20[01][0123456789](01|02|03|04|05|06|07|08|09|10|11|12)$/) {
      ddl += "PARTITIONED BY (month string) \n"
        apl += addPartitions(table, partitions, "month") + "\n"
    }
    else if (partitions == "month" || partitions == "year"||partitions == "quarter") {  /*Added by Arvind*/
      ddl +="PARTITIONED BY (" + partitions + " string) \n"
   }
    else {
      throw new RuntimeException("Incorrect pattern for partitions")
    }

      ddl += "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' \n"
      ddl += "STORED AS " + fileformat + " \n"
      ddl += "LOCATION '" + hdfspath + "'\n"
      ddl += "tblproperties ("
      ddl += "'serialization.null.format'='',"
      ddl += "'escape.delim'='\\\\',"
      ddl += "'field.delim'='|'"
      if ( fileformat == 'AVRO' ) 
      {
       
        ddl += ",'avro.schema.url'='hdfs://" + raw_landing_path + "/" +  interface_nm.tokenize('_')[0] +  "/AVRO_HEADER/" +  src_schema + "/" + table + ".avsc'"
      }
      ddl += ") \n\n;"

      ddl += apl

      new File(folder.getAbsolutePath() + "/" + table + "_raw.sql").write(ddl)
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
              if (s != '00' && n <= 12) list << it
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
      apl += "alter table " + table + "_raw add if not exists partition (" + frequency + "='" + it + "');\n"
    }

    apl
  }

}
