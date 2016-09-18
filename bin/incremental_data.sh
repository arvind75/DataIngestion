#!/bin/bash

$debug && set -x

function usage  {

echo "incremental_data.sh -s <source schema> -t <source tablename> "
exit 1;
}

while getopts "s:t:n:h?" opt
do
case ${opt}  in
  s) schema=$OPTARG;;
  t) tab_name=$OPTARG;;
  n) svc_name=$OPTARG;;
  f) header_file=$OPTARG;;
  *) usage;
     exit ;;

esac
done

typeset -u tab_name schema svc_name


sqoop_data.sh

echo "Extracting Header Information for ${src_schema} ${src_tab_nm}"
echo "groovy jdbc_connect_oracle.groovy ${header_file} ${username} xxxxxx  jdbc:${source_db_typ}:thin:@${hostname}:${port}/${db_name} ${src_schema} ${src_tab_nm} "

groovy jdbc_connect_oracle.groovy ${header_file} ${username} ${password} "jdbc:${source_db_typ}:thin:@${hostname}:${port}/${db_name}" ${src_schema} ${src_tab_nm} > ${header_file}

groovy ConvertRawData.groovy ${header_file} ${hive_db_nm} ${target_dir} none

hive -v -f ${src_tab_nm}_raw.sql

done
