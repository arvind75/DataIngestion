#!/bin/bash

$debug && set -x

function usage  {

echo "create_raw_table.sh -s <source schema> -t <source tablename> -h <header file name> -d <hdfs path> -p <partition> -r <refresh type>"
exit 1;
}

# Groovy script arguments
#    String table = args[0]; //table name
#    String interface_nm = args[1]; //hive db; Interface Name
#    String fileformat = args[2]; //file format
#    String hdfspath = args[3]; //hdfs staging area
#    String header_file=arg[4]; //header file name with path
#    String partitions = args[5];  // Partitions
#    String incr_flag = args[6];  // Refresh Type <incr/fulli/append>
#    String src_schema = args[7];  // Source schema name


while getopts "s:t:h:d:p:r:i:f:?" opt
do
case ${opt}  in
  s) schema=$OPTARG;;       #Schema of the source table
  t) tab_name=$OPTARG;;     #Table name
  h) header_file=$OPTARG;;  # Header file of the table that would be created in the hive staging schema
  d) hdfs_dir=$OPTARG;;     #HDFS file path where data would reside for external table
  p) partition=$OPTARG;;    #Partition column name
  r) refresh_type=$OPTARG;; #Type of refesh full, incr, append
  f) fileformat=$OPTARG;;   #fileformat of hive table
  i) interface_nm=$OPTARG;; #Interface dir on hadoop
  *) usage;
     exit ;;
esac
done

if [ $# -lt 6 ]; then
  echo "Arguments missing ....exiting"
  exit 1;
fi;

typeset -u tab_name schema  file_compress import_file_format ;
typeset -l partition;


header_file=${header_file:-"${schema}.${tab_name}.txt"}
fileformat=${STAGING_FILEFORMAT:-${fileformat}}

groovy ${FUNC_HOME?}/Convertddl.groovy ${tab_name} ${interface_nm} ${fileformat} ${hdfs_dir} ${header_file} ${partition} ${refresh_type} ${schema}
if [ $? -ne 0 ]; then
  logit ${LOG_FILE} ERROR "Execution of ${FUNC_HOME?}/Convertddl.groovy Failed"
  exit 1;
fi;

echo "Creating hive database ${interface_nm}";
hive -v -e "create database IF NOT EXISTS ${interface_nm}"

hive -v -f ${OUT_HOME?}/${schema?}.${tab_name}/${tab_name}.sql
if [ $? -ne 0 ]; then
  logit ${LOG_FILE} ERROR "Execution of ${OUT_HOME?}/${schema?}.${tab_name}/${tab_name}.sql Failed"
  exit 1;
fi;

