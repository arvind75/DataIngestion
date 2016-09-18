#!/bin/bash

$debug && set -x

function usage  {

echo "create_raw_table.sh -s <source schema> -t <source tablename> -h <header file name> -d <hdfs path> -p <partition> -r <refresh type> -i <Interface Name>"
exit 1;
}

# Groovy script arguments
#    String table = args[0]        // Header (CSV) File
#    String interface_nm = args[1]    // Hive Database
#    String hdfspath = args[2]    // HDFS Path for Raw Data
#    String header_file=args[3]   // Header File
#    String partitions = args[4]  // Partitions
#    String fileformat=arg[5]   //fileformat
#    String incr_flag = args[6]  // refresh type <incr/full>
#    String src_schema = args[7]  // Source schema name


while getopts "s:t:h:d:p:r:i:f:o:?" opt
do
case ${opt}  in
  s) schema=$OPTARG;;       #Schema of th source e table
  t) tab_name=$OPTARG;;     #Table name
  h) header_file=$OPTARG;;  # Header file of the table that would be created in the hive staging schema
  d) hdfs_dir=$OPTARG;;     #HDFS file path where data would reside for external table
  p) partition=$OPTARG;;    #Partition column name
  r) refresh_type=$OPTARG;; #Type of refesh full, incr, append
  f) fileformat=$OPTARG;;   #fileformat of hive table
  i) interface_nm=$OPTARG;; #Interface Name
  o) src_db_typ=$OPTARG;;  #Source DB type
  *) usage;
     exit ;;
esac
done

typeset -u tab_name schema  file_compress import_file_format ;
typeset -l partition;

header_file=${header_file:-"${schema}.${tab_name}.txt"}
fileformat=${RAW_FILEFORMAT:-${fileformat}}

if [ "${fileformat?}" = "AVRO" ]; then
  ${SCRIPT_HOME}/hfs -mkdir -p ${RAW_LANDING_PATH?}/${interface_nm?}/AVRO_HEADER/${schema?}
  if [ $? -ne 0 ]; then
    logit $LOG_FILE ERROR "Cannot create directory ${RAW_LANDING_PATH?}/${interface_nm?}/AVRO_HEADER"
  fi;

  if [ ${${SCRIPT_HOME}/hls ${RAW_LANDING_PATH?}/${interface_nm?}/AVRO_HEADER/${schema?}/${tab_name?}.avsc|awk '{print NR}') -gt 0 ]; then
    ${SCRIPT_HOME}/hfs -mv ${RAW_LANDING_PATH?}/${interface_nm?}/AVRO_HEADER/${schema?}/${tab_name?}.avsc  \
                           ${RAW_LANDING_PATH?}/${interface_nm?}/AVRO_HEADER/${schema?}/${tab_name?}.avsc.${batch_id}
##  ${SCRIPT_HOME}/hfs -rm -f ${RAW_LANDING_PATH?}/${interface_nm?}/AVRO_HEADER/${schema?}/${tab_name?}.avsc
  fi


  if [ "${src_db_typ}" = "oracle" ]; then
    ${SCRIPT_HOME}/hfs -put ${OUT_HOME?}/${schema?}.${tab_name?}/${schema?}_${tab_name?}.avsc ${RAW_LANDING_PATH?}/${interface_nm?}/AVRO_HEADER/${schema?}/${tab_name?}.avsc
  elif [ "${src_db_typ}" = "sqlserver" ]; then
    ${SCRIPT_HOME}/hfs -put ${OUT_HOME?}/${schema?}.${tab_name?}/${tab_name?}.avsc ${RAW_LANDING_PATH?}/${interface_nm?}/AVRO_HEADER/${schema?}/${tab_name?}.avsc
  elif [ "${src_db_typ}" = "db2" ]; then
    ${SCRIPT_HOME}/hfs -put ${OUT_HOME?}/${schema?}.${tab_name?}/${tab_name?}.avsc ${RAW_LANDING_PATH?}/${interface_nm?}/AVRO_HEADER/${schema?}/${tab_name?}.avsc
  elif [ "${src_db_typ}" = "mysql" ]; then
    ${SCRIPT_HOME}/hfs -put ${OUT_HOME?}/${schema?}.${tab_name?}/${tab_name?}.avsc ${RAW_LANDING_PATH?}/${interface_nm?}/AVRO_HEADER/${schema?}/${tab_name?}.avsc
  fi

  if [ $? -ne 0 ]; then
    logit $LOG_FILE ERROR "Cannot put file ${OUT_HOME?}/${schema?}.${tab_name?}/${tab_name?}.avsc into  ${RAW_LANDING_PATH?}/${interface_nm?}/AVRO_HEADER/${schema?}/${tab_name?}.avsc"
    exit 1
  fi;
fi;

if [ ${ONLY_RAW_PROCESSING?} -eq 1 ]; then
  if [ ${partition?} = "none" ]; then
    part_clause=""
  else 
    part_clause="partition by (${partition} string)"
  fi;
  hive --silent=true -e  "DROP TABLE ${interface_nm?}.${tab_name?}; CREATE EXTERNAL TABLE ${interface_nm?}.${tab_name?} ${part_clause?} stored as ${fileformat} \
        location '${hdfs_dir?}' \
        tblproperties ('avro.schema.url'='${RAW_LANDING_PATH?}/${interface_nm?}/AVRO_HEADER/${schema?}/${tab_name?}.avsc')"
  exit 0;
fi;

groovy ${FUNC_HOME?}/ConvertRawData.groovy ${tab_name} ${interface_nm} ${hdfs_dir}  ${header_file} ${partition} ${fileformat} ${refresh_type} ${schema}
if [ $? -ne 0 ]; then
  logit ${LOG_FILE} ERROR "Execution of ${FUNC_HOME?}/ConvertRawData.groovy Failed"
  exit 1;
fi;

echo "Creating ${schema?}_raw database in hive"
hive -v -e "create database IF NOT EXISTS ${interface_nm?}_raw;"

hive -v -f ${OUT_HOME?}/${schema?}.${tab_name}/${tab_name}_raw.sql
if [ $? -ne 0 ]; then
  logit ${LOG_FILE} ERROR "Execution of ${OUT_HOME?}/${schema?}.${tab_name}/${tab_name}_raw.sql Failed"
  exit 1;
fi;

