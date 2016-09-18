#!/bin/bash

function usage  {

echo "create_raw_table.sh -s <source schema> -t <source tablename> -h <header file name> -d <hdfs path> -p <partition> -r <refresh type>"
exit 1;
}

while getopts "s:t:?" opt
do
case ${opt}  in
  s) src_schema=$OPTARG;;   #Schema of the table
  t) tab_name=$OPTARG;;     #Table name
  *) usage;
          exit ;;
esac
done

echo hive -v -f ${OUT_HOME?}/${src_schema}.{tab_name}/${tab_name}_cleanup.sql
