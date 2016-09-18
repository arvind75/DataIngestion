#!/bin/bash

${debug} && set -x


function usage {

echo "create_incr_update.sh -s <source schema> -t <source tablename> -h <header file name> -p < Is partition [none|value|Column Name]> -k <Primary Key column seperated by space>"
exit 1;
}

while getopts "i:t:k:h:p:s:r:?" opt
do
case ${opt}  in
  i) interface_nm=$OPTARG;;
  t) tab_name=$OPTARG;;
  k) pk=$OPTARG;;
  h) header_file=$OPTARG;;
  p) partition=$OPTARG;;   #Partition Column name
  s) src_schema=$OPTARG;;
  r) refresh_typ=$OPTARG;;
  *) usage;
     exit ;;
esac
done

typeset -u tab_name interface_nm svc_name file_compress import_file_format where_clause ;
typeset -l partition;

for column in ${pk?}
do
 where_clause1=$(echo "incr.${column} = orig.${column} and ${where_clause1}")
done
where_clause=$(echo ${where_clause1}|sed "s/and$//g")

default_time=$(date '+%Y-%m-%d %H:%M:%S')

if [ "${partition}" = "none" ]; then
  part_clause=""
else
  part_clause="partition (${partition})"
fi;
  

echo "use ${interface_nm?};"

if [ ${refresh_typ?} = "incr" ]; then

  for  tab_col  in $(cat ${HEADER_HOME?}/${header_file?}|awk ' BEGIN {FS=","}{if (NR >1) print $1}')
  do
    col_name1=$(echo ${col_name1}, "IF (INCR.${tab_col} IS NOT NULL, INCR.${tab_col}, ORIG.${tab_col} )")
  done 
  col_name=$(echo $col_name1|awk '{print substr($0,2)}')
  
  {
     echo "use ${interface_nm?};"
     echo " INSERT OVERWRITE TABLE ${interface_nm?}.${tab_name?} ${part_clause} SELECT "
     echo "${col_name}"|awk ' BEGIN {FS="),"}{for (i=1;i<=NF ; i++){ if (i == NF) {print $i } else {print $i "),"}}}'
     echo " FROM ${interface_nm}.${tab_name?}_INCR  INCR FULL OUTER JOIN ${interface_nm}.${tab_name?} ORIG ON ("
     echo " ${where_clause?});"|awk ' BEGIN {FS="AND"}{for (i=1;i<=NF ; i++){ if (i == NF) {print $i } else {print $i "AND"}}}'
  }  > $OUT_HOME/${src_schema}.${tab_name}/${tab_name}_update.hql

else 
   echo "Incorrect refresh type provided....exiting"
   exit 1;
fi;

hive -v -f ${OUT_HOME?}/${src_schema?}.${tab_name}/${tab_name}_update.hql
if [ $? -eq 0 ]; then
   echo "Update for Incremental records successful on ${src_schema}.${tab_name}"
else
   echo "Update for Incremental records failed on ${src_schema}.${tab_name}"
   exit 1;
fi
