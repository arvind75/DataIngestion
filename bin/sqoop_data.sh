#!/bin/bash

$debug && set -x

function usage {

echo "Usage: sqoop_data.sh  
       -s <source schema> 
       -t <source tablename> 
       -n <Service name>
       -d <sqoop target directory. This can be the raw dir in case raw it the final location>
       -p <Partition value eg. month=2014|quarter=201201>
       -m <Extract parallelism. if used, parallel_splitby is mandatory>
       -b <parallel_splitby column value which will be used to split records across parallel streams>
       -f <Import format : AVRO, TEXTFILE, SEQ, PARQUET>
       -c <Compression required Y|N>
       -o <SkipSetup required in table for sqoop>
       -q <Query used against the table >
       -u <username of the source>
       -w <Password for the source>
       -h <Where predicate based upon incremental column name condition>
       -l >Incremental column name with condition like \"CREATE_DT >\" or \"CREATE_DT =\"
       -i <interface name>"
exit 1;
}

while getopts "s:t:n:d:p:m:f:q:u:w:i:h:l:co?" opt
do
case ${opt}  in
  s) schema=$OPTARG;;
  t) tab_name=$OPTARG;;
  n) svc_name=$OPTARG;;
  d) sqoop_directory=$OPTARG;;  #Temp Dir for sqoop from where data will be picked. For RAW only procesing, this can be the RAW dir location
  p) partition=$OPTARG;;        #Partition Value to extract
  m) parallelism=$OPTARG;;
  b) splitby=$OPTARG;;
  f) import_format=$OPTARG;;
  c) compress=Y;;
  o) skipSetup=Y;;
  q) query_str=$OPTARG;;
  u) user=$OPTARG;;
  w) pass=$OPTARG;;
  i) int_nm=$OPTARG;;
  h) where_predicate=$OPTARG;;
  l) incr_col_nm_cond=$OPTARG;;
  *) usage;
     exit ;;

esac
done

typeset -u tab_name schema svc_name file_compress import_file_format ;
typeset -l source_db_typ refresh_typ;

default_time=$(date '+%Y-%m-%d %H:%M:%S')
skipSetup=${skipSetup:-"N"};


if [ "${skipSetup}" = "N" ]; then 

  table_info=$(mysql -u${DB_USER} -p$(echo ${DB_PASS}|${SCRIPT_HOME}/password -d) -h${DB_HOST} -D dataload --skip-column-names \
            -e "select CONCAT(a.SERVICE_NM,'|',a.SOURCE_DB_TYP,'|',a.HOSTNAME,'|', \
            a.PORT,'|',a.DB_NAME,'|',a.SOURCE_DB_TYP,'|',a.username,'|',a.password,'|', \
            COALESCE(b.EXTRACT_ID,0),'|',COALESCE(b.EXTRACT_NAME,'NULL'),'|',COALESCE(b.CREATE_JOB,'NULL'),'|',COALESCE(b.SRC_SCHEMA,'NULL'),'|', \
            COALESCE(b.SRC_TAB_NM,'NULL'),'|',COALESCE(b.REFRESH_TYP,'NULL') ,'|', COALESCE(b.INCR_PK_COL,'NULL') ,'|', \
            COALESCE(b.INCR_TAB_NM,'NULL'),'|',COALESCE(b.INCR_COL_NM,'NULL'),'|', COALESCE(b.INCR_COL_EQUATION,'NULL'),'|',COALESCE(b.IS_PARTITION,'NULL'),'|', \
            COALESCE(b.PARTITION_COL_NM,'NULL') ,'|', COALESCE(b.PARALLEL_EXTRACT,'NULL') ,'|',COALESCE(b.EXTRACT_PARALLELISM,1),'|', \
            COALESCE(b.PARALLEL_SPLITBY,'NULL'),'|',COALESCE(b.QUERY,'NULL'),'|', \
            COALESCE(b.IMPORT_FILE_FORMAT,'NULL'),'|',COALESCE(b.FILE_COMPRESS,'NULL'),'|',COALESCE(b.SERVICE_NM,'NULL'),'|',COALESCE(INTERFACE_NM,'NULL')) \
            from extract_tab_config b, source_config a where a.service_nm=b.service_nm and  b.SRC_SCHEMA='${schema}' \
            and b.SRC_TAB_NM='${tab_name}' and a.SERVICE_NM = '${svc_name}' limit 1;")

#  table_info=$(mysql -u${DB_USER} -p$(echo ${DB_PASS}|${SCRIPT_HOME}/password -d) -h${DB_HOST} -D dataload --skip-column-names \
#            -e "select a.SERVICE_NM,a.SOURCE_DB_TYP,a.HOSTNAME, \
#            a.PORT,a.DB_NAME,a.SOURCE_DB_TYP,a.username,a.password, \
#            COALESCE(b.EXTRACT_ID,0),COALESCE(b.EXTRACT_NAME,'NULL'),COALESCE(b.CREATE_JOB,'NULL'),COALESCE(b.SRC_SCHEMA,'NULL'), \
#            COALESCE(b.SRC_TAB_NM,'NULL'),COALESCE(b.REFRESH_TYP,'NULL') , COALESCE(b.REFRESH_SUB_TYP,'NULL') , \
#            COALESCE(b.INCR_TAB_NM,'NULL'),COALESCE(b.INCR_COL_NM,'NULL'), COALESCE(b.MERGE_KY,'NULL'),COALESCE(b.IS_PARTITION,'NULL'), \
#            COALESCE(b.PARTITION_COL_NM,'NULL') , COALESCE(b.PARALLEL_EXTRACT,'NULL') ,COALESCE(b.EXTRACT_PARALLELISM,1), \
#            COALESCE(b.PARALLEL_SPLITBY,'NULL'),COALESCE(b.QUERY,'NULL'), \
#            COALESCE(b.IMPORT_FILE_FORMAT,'NULL'),COALESCE(b.FILE_COMPRESS,'NULL'),COALESCE(b.SERVICE_NM,'NULL'),COALESCE(INTERFACE_NM,'NULL') \
#            from extract_tab_config b, source_config a where a.service_nm=b.service_nm and  b.SRC_SCHEMA='${schema}' \
#            and b.SRC_TAB_NM='${tab_name}' and a.SERVICE_NM = '${svc_name}' limit 1;")

fi;

create_job=${create_job:-"N"};
compress=${compress:-"N"};
import_format=${import_format:-"AVRO"};
LOG_FILE=${LOG_FILE:-"${LOG_HOME}/sqoop_log"}



if [[ ( -z "${table_info}") && ("${skipSetup}" = "N") ]]; then
  echo "Table not configured in the EXTRACT_TABL_CONFIG...please configure before executing..."
  exit 1;
fi;


echo ${table_info}| 
       while  IFS="|"  read  service_nm source_db_typ1 hostname port db_name source_db_typ username password \
       extract_id extract_name create_job src_schema src_tab_nm refresh_typ incr_pk_col \
       incr_tab_nm incr_col_nm incr_col_equation is_partition \
       partition_col_nm parallel_extract extract_parallelism parallel_splitby query \
       import_file_format file_compress service_nm interface_nm
do


  typeset -u src_schema src_tab_nm;
  typeset -l source_db_typ;
  src_schema=${src_schema:-${schema}};
  src_tab_nm=${src_tab_nm:-${tab_name}};
  file_compress=${file_compress:-${compress}};
  import_file_format=${import_file_format:-${import_format}};
  parallel_splitby=${parallel_splitby:-${splitby}};
  extract_parallelism=${extract_parallelism:-${parallelism}};
  username=${username:-${user}};
  password=${password:-${pass}};
  incr_col_nm=${incr_col_nm:-${incr_col_nm_cond}};

  if [ -z ${int_nm} ]; then
    interface_nm=${interface_nm};
  else
    interface_nm=${int_nm};
  fi;

  if [ -z  ${sqoop_directory} ]; then 
    sqoop_dir=${SQOOP_LANDING_PATH:-"/user/tmp/sqoop"}/${src_schema}
  else
    sqoop_dir=${sqoop_directory?}/${interface_nm?}
  fi;

  if [ -n "${partition}" ]; then
    target_dir=${sqoop_dir}/${src_tab_nm}/${partition}
  else
    target_dir=${sqoop_dir}/${src_tab_nm}
  fi;
  

  if [ "incr" = "${refresh_typ}" ]; then
    if [[ "NULL" = "${where_predicate}" || "NULL" = "${incr_col_nm}" ]]; then
      echo "No where predicate supplied or incr column name, cannot perform incremental pull from source"
      exit 1;
    fi;
    sqoop_option_where="--where "
    sqoop_option="${incr_col_nm?}  ${incr_col_equation?}  '${where_predicate?}'"
  fi;

#  if [ "INCR" = "${refresh_typ}" ]; then
#    if   [ 'M' = ${refrsh_sub_typ} ]; then
#       sqoop_option="merge --new-data ${incr_file_dir} --onto ${target_dir} merged --jar-file datatypes.jar --class-name Foo --merge-key ${merge_ky}"
#    elif [ 'A' = ${refrsh_sub_typ} ]; then
#       sqoop_option="--incremental lastmodified --check-column ${incr_col_nm} "
#   fi;
#  elif [ 'FULL' =  "${refresh_typ}" ]; then
#       sqoop_option="--append"
#  else
#    echo "Invalid Refresh type entered...exiting"
#    exit 1;
#  fi;
  
  if [[ ${extract_parallelism} -gt 1 ]]; then
    if [ "${parallel_splitby}" = "NULL" ]; then
       echo "parallel_splitby argument missing...exiting"
       exit 1;
    fi;
    parallel="-m ${extract_parallelism}"
    parallel_clause=" ${parallel} --split-by ${parallel_splitby}"
  else
    parallel_clause="-m 1"
  fi;
  
  if [ 'Y' = "${create_job}" ]; then
    create_option="job --create ${extract_name}"
  fi;

  echo $import_file_format;

  if [ 'Y' = "${file_compress}" ]; then
    case ${import_file_format} in 
       AVRO)
          compress="--compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec";;
       PARQUET)
          compress="--compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec";;
       TEXTFILE)
         compress="--compress --compression-codec org.apache.hadoop.io.compress.GzipCodec";;
       SEQ)
          compress="--compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec";;
       *) compress="" ;;
    esac
  fi;

  case ${import_file_format} in
  AVRO)
      format_str="--as-avrodatafile";;
  TXT)
      format_str="--as-textfile";;
  SEQ)
      format_str="--as-sequencefile";;
  PARQUET)
      format_str="--as-parquet";;
  *) format_str="" ;;
  esac

  
  if [ -n "${query_str}" ]; then
    query_sql=" \"${query_str} WHERE \$CONDITIONS\""
    table_str=""
  elif [[  "NULL" !=  "${query}" && "${query}" != "" ]]; then
    query_sql="\"${query} WHERE \$CONDITIONS \""
  elif [ -f ${QUERY_HOME}/${src_schema}.${src_tab_nm}.query ]; then
    qry_str=$(cat ${QUERY_HOME}/${src_schema}.${src_tab_nm}.query)
    query_sql="\"${qry_str} WHERE \$CONDITIONS \""
    table_str=""
  else
    query_sql=""
#    table_str="--table ${src_schema}.${src_tab_nm}"  #Removed schema as sqlserver sqoop were failing with schema
    table_str="--table ${src_tab_nm}"
  fi;
  
  echo "Creating dummy directory"
  ${SCRIPT_HOME}/hfs -mkdir -p ${target_dir}
  if [ $? -ne 0 ]; then
    logit ${LOG_FILE} ERROR  "Creating dummy dir from specified target directory : ${target_dir} failed"
    exit 1;
  fi;



  echo "Exporting data for table ${src_schema}.${src_tab_nm} using Sqoop...\n"
  
  echo "Removing files in the specified target directory : ${target_dir}"
  ${SCRIPT_HOME}/hfs -rm -r -f -skipTrash ${target_dir}
  if [ $? -ne 0 ]; then
    logit ${LOG_FILE} ERROR  "Removing files from specified target directory : ${target_dir} failed"
    exit 1;
  fi;

###############################################################################################################
###Changes to be made for new DBMS added to the script for handling SkipSetup and DBMS setup

  if [ "${skipSetup}" = "Y" ]; then              #Skip mysql dataload schema setup and just do sqoop of data standalone
    if [ -f ${PROJ_HOME?}/setup/source_dtl.txt ]; then
      connect_str=$(cat ${PROJ_HOME?}/setup/source_dtl.txt|grep -v "#")
      if [ $(cat ${PROJ_HOME?}/setup/source_dtl.txt|grep -v "#"|awk '{if ($0 ~ /oracle/ ) {print 0} else{ print 1}}') -eq 0 ]; then
        table_str="--table ${src_schema}.${src_tab_nm}"
      elif [ $(cat ${PROJ_HOME?}/setup/source_dtl.txt|grep -v "#"|awk '{if ($0 ~ /sqlserver/ ) {print 0} else {print 1}}') -eq 0 ]; then
        table_str="--table ${src_tab_nm} -- --schema ${src_schema}"
      fi;
    else
      logit ${LOG_FILE} ERROR  "Setup file not confiured for connection details"
    fi;
  else 
    if [ "${source_db_typ}" = "oracle" ]; then
      connect_str="jdbc:${source_db_typ}:thin:@${hostname}:${port}/${db_name}"
      driver_str="--driver \"oracle.jdbc.driver.OracleDriver\""
      if [ -z ${query_sql} ]; then               #Adding this as schema identifier works for oracle and not for sqlserver
         table_str="--table ${src_schema}.${src_tab_nm}"
         table_schema=""
      fi;
    elif [ "${source_db_typ}" = "sqlserver" ]; then
      connect_str="jdbc:${source_db_typ}://${hostname}:${port};databaseName=${db_name}"
      driver_str="--driver \"com.microsoft.sqlserver.jdbc.SQLServerDriver\""
      if [ -z ${query_sql} ]; then               #Adding this as schema identifier works for oracle and not for sqlserver
         table_str="--table ${src_tab_nm}"
         table_schema="-- --schema ${src_schema}"
      fi;
    elif [ "${source_db_typ}" = "db2" ]; then
      connect_str="jdbc:${source_db_typ}://${hostname}:${port}/${db_name}"
      driver_str="--driver \"com.ibm.db2.jcc.DB2Driver\""
      if [ -z ${query_sql} ]; then               #Adding this as schema identifier works for oracle and not for sqlserver
         table_str="--table ${src_tab_nm}"
         table_schema="-- --schema ${src_schema}"
      fi;
    elif [ "${source_db_typ}" = "mysql" ]; then
      connect_str="jdbc:${source_db_typ}://${hostname}:${port};databaseName=${db_name}"
      driver_str="--driver \"com.mysql.jdbc.Driver\""
      if [ -z ${query_sql} ]; then               #Adding this as schema identifier works for oracle and not for sqlserver
         table_str="--table ${src_tab_nm}"
         table_schema="-- --schema ${src_schema}"
      fi;

    
    fi
  fi;

################################################################################################################
  
  set -f  #used for overriding the asterisk from string

  if [ -n "${query_sql}" ]; then
      echo "Found query definition defined"
      echo "/usr/bin/sqoop  ${create_option} import --direct ${driver_str} --query "${query_sql}" --target-dir ${target_dir}  \
      ${table_str} --outdir ${OUT_HOME}/${src_schema}.${src_tab_nm} --connect  \"${connect_str?}\"  \
      --username ${username} --password xxxxx ${parallel_clause} \
      --fields-terminated-by \"\\${DATA_DEL}\" --escaped-by \\\\ ${sqoop_option}  ${format_str} ${compress} ${table_schema}"

      /usr/bin/sqoop  ${create_option} import --direct ${driver_str}  --query "${query_sql}" --target-dir ${target_dir}  \
      ${table_str} --outdir ${OUT_HOME}/${src_schema}.${src_tab_nm} --connect  "${connect_str?}"  \
      --username ${username} --password $(echo "${password}"|${SCRIPT_HOME}/password -d) ${parallel_clause} \
      --fields-terminated-by \"\\${DATA_DEL}\" --escaped-by \\\\ ${sqoop_option}  ${format_str} ${compress} ${table_schema}
  else
      echo "Missing query definition, defaulting to complete table"
    
      echo "      /usr/bin/sqoop  ${create_option} import --direct ${driver_str} --target-dir ${target_dir}  \
      ${table_str} --outdir ${OUT_HOME}/${src_schema}.${src_tab_nm} --connect  \"${connect_str?}\"  \
      --username ${username} --password xxxxx ${parallel_clause} \
      --fields-terminated-by \"\\${DATA_DEL}\" --escaped-by \\\\  --where  "${sqoop_option}"  ${format_str} ${compress} ${table_schema}"

      /usr/bin/sqoop  ${create_option} import --direct ${driver_str} --target-dir ${target_dir}  \
      ${table_str} --outdir ${OUT_HOME}/${src_schema}.${src_tab_nm} --connect  ${connect_str?}  \
      --username ${username} --password $(echo "${password}"|${SCRIPT_HOME}/password -d) ${parallel_clause} \
      --fields-terminated-by \"\\${DATA_DEL}\" --escaped-by \\\\ --where   "${sqoop_option}"  ${format_str} ${compress} ${table_schema}
  fi;

  if [ $? -eq 0 ]; then
    logit ${LOG_FILE} INFO  "Sqoop of data from ${src_schema}.${src_tab_nm} Successful"
  else
    logit ${LOG_FILE} ERROR "Sqoop of data from ${src_schema}.${src_tab_nm} Failed"
    exit 1;
  fi;

  ${SCRIPT_HOME}/extract_header.sh -s ${src_schema} -t ${src_tab_nm} -u ${username} -p ${password} -c ${connect_str?} -d ${source_db_typ}
  if [ $? -ne 0 ]; then
    logit ${LOG_FILE} ERROR  "Header extract  from ${src_schema}.${src_tab_nm} Failed"
    exit 1;
  fi;

done
