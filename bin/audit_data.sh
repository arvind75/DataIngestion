#!/bin/bash

$debug && set -x 

function usage  {

echo "audit_data.sh -s <source schema> -t <source tablename> -p <partition> -r <refresh type>"
exit 1;
}

while getopts "b:s:t:p:r:i:h:?" opt
do
case ${opt}  in
  b) batch_id=$OPTARG;;
  e) extract_id=$OPTARG;;
  s) src_schema=$OPTARG;;   #Schema of the table
  t) tab_name=$OPTARG;;     #Table name
  p) partition=$OPTARG;;    #Partition column name
  r) refresh_type=$OPTARG;; #Type of refesh full, incr, append
  i) interface_nm=$OPTARG;; #Interface Name in hdfs
  t) start_tm=$OPTARG;;
  d) end_tm=$OPTARG;;
  l) elapse_tm=$OPTARG;;
  *) usage;
          exit ;;
esac
done

typeset -u tab_name interface_nm  ;
typeset -l partition;


if [ "${refresh_type}" = "FULL" ];then
  partitin_clause=""
  if [ "${partition}" != "none" ]; then
    if [ $(awk '{ if (partition  ~ /^[0-9]/ ) }') ]; then
       partitin_clause="where ${partition?}"
    else
       partition_clause=""
    fi;l
  fi;
else
  if [ "${refresh_type}" = "INCR" ]; then
    incr_col_val=${incr_col_val}
  fi;
  
fi

src_rows=$(grep "Map output records" ${LOG_FILE}|awk ' BEGIN {FS="="}{print $2}')
tgt_rows=$(hive --silent -e "select count(*) from ${interface_nm}.${tab_name})

insert into extract_audit \
    (EXEC_ID,EXTRACT_ID, STG_TAB_NM, SOURCE_ROW, REFRESH_TYP, TGT_ROWS,INCR_COL_VAL,EXEC_STRT_TS,EXEC_END_TS,EXEC_LAPSE_TS,STATUS) values \
    (${batch_id?},${extract_id?},${tab_name?},${src_rows?},${refresh_type?},${tgt_rows?},${incr_col_val?},${start_ts},${end_ts},${elapse_ts},${status});


