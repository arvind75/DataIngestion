#!/bin/bash

$debug && set -x

function usage  {

echo "create_raw_table.sh -s <source schema> -t <source tablename> -h <header file name> -d <hdfs path> -p <partition> -r <refresh type>"
exit 1;
}

# Groovy script arguments
#    String header_file = args[0];
#    String interface_nm = args[1];
#    String table = args[2];
#    String partitions = args[3];
#    String incr_flag = args[4];
#    String scr_schema = args[5];


while getopts "s:t:p:r:i:h:?" opt
do
case ${opt}  in
  s) src_schema=$OPTARG;;   #Schema of the table
  t) tab_name=$OPTARG;;     #Table name
  h) header_file=$OPTARG;;  # Header file of the table that would be created in the hive staging schema
  p) partition=$OPTARG;;    #Partition column name
  r) refresh_type=$OPTARG;; #Type of refesh full, incr, append
  i) interface_nm=$OPTARG;; #Interface Name in hdfs
  *) usage;
          exit ;;
esac
done

if [ $# -lt 4 ]; then
  echo "Arguments missing ....exiting"
  exit 1;
fi;

typeset -u tab_name interface_nm  ;
typeset -l partition;


header_file=${header_file:-"${src_schema}.${tab_name}.txt"}
fileformat=${fileformat:-${STAGING_FILEFORMAT}}

groovy ${FUNC_HOME?}/TransformRAWData.groovy ${header_file} ${interface_nm} ${tab_name} ${partition} ${refresh_type} ${src_schema}
if [ $? -ne 0 ]; then
  logit ${LOG_FILE} ERROR "Execution of ${FUNC_HOME?}/TransformRAWData.groovy Failed"
  exit 1;
fi;

if [ -f ${UDF_HOME?}/hivecleaningudf-0.0.1-SNAPSHOT.jar ]; then
  cp -p ${UDF_HOME?}/hivecleaningudf-0.0.1-SNAPSHOT.jar ${OUT_HOME?}/${src_schema}.${tab_name?}/.
  export HIVE_AUX_JARS_PATH=${UDF_HOME?}/hivecleaningudf-0.0.1-SNAPSHOT.jar 
fi;


hive -v -f ${OUT_HOME?}/${src_schema?}.${tab_name?}/${tab_name?}_cleanup.sql
if [ $? -ne 0 ]; then 
  logit ${LOG_FILE} ERROR "Execution of ${OUT_HOME?}/${src_schema?}.${tab_name?}/${tab_name?}_cleanup.sql Failed"
  exit 1;
fi;


#Execution happens in a different script
#if [ "$refresh_type" = "incr" ];then
#  echo hive -v -f ${OUT_HOME?}/${src_schema}.{tab_name}/${tab_name}_cleanup.sql
#  echo hive -v -f ${OUT_HOME?}/${src_schema}.{tab_name}/${tab_name}_dedup.sql
#else
#  echo hive -v -f ${OUT_HOME?}/${src_schema}.{tab_name}/${tab_name}_cleanup.sql
#fi
