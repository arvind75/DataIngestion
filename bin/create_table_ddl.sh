#!/bin/bash

function usage  {

echo "$(basename $0) -r <hdfs raw path> -s <hdfs stage path> -p <partition  [month|year|quarter]> -i <incremental [incr|full] -h <header file name> -d <hive db name>"
echo "create_table_ddl.sh  -r /user/arvind -s /user/arvind/stg -p month -i full -t bw_phmmpuir -d arvind"
exit 1;
 }

while getopts "r:s:p:i:t:d:?" opt
do
case ${opt}  in
  r) hdfs_raw_area=$OPTARG;;
  s) hdfs_staging_area=$OPTARG;;
  p) partitions=$OPTARG;;
  i) refresh_type=$OPTARG;;
  t) table=$OPTARG;;
  d) hive_database=$OPTARG;;
  *) usage
     exit  ;;

esac
done


refresh_type=${refresh_type:="full"}


groovy ${FUNC_HOME}/ConvertRawData ${table} ${hive_database} ${hdfs_raw_area} ${partitions} ${refresh_type}
groovy ${FUNC_HOME}/Convertddl ${table} ${hive_database} noavro ${hdfs_staging_area} ${partitions} ${refresh_type}

if [ ${refresh_type} = "incr" ]; then
  cp -p ${OUT_HOME?}/${table?}/${table}.sql ${OUT_HOME?}/${table?}/${table}_new.sql
  perl -pi -e  "s/${table}/${table}_new/g" ${OUT_HOME?}/${table?}/${table}_new.sql
  groovy ${FUNC_HOME}/Convertddl ${table} ${hive_database} noavro ${hdfs_staging_area} ${partitions} full
  

echo "use ${hive_database};
echo "CREATE VIEW ${table} AS "
echo "SELECT t1.* FROM "
echo "(SELECT * FROM ${table}
echo "UNION ALL
echo "SELECT * FROM ${table}_incr) t1
echo "JOIN
echo "(SELECT 0comp_code, 0fiscper, 0fiscvarnt, 0ac_doc_no, 0item_num, 0fi_dsbitem, 0ven_compc, 0calmonth, max(zloaddate) zloaddate
echo "FROM
echo "(SELECT * FROM bw_phfiapm02
echo "UNION ALL
echo "SELECT * FROM bw_phfiapm02_incr) t2
echo "GROUP BY 0comp_code, 0fiscper, 0fiscvarnt, 0ac_doc_no, 0item_num, 0fi_dsbitem, 0ven_compc, 0calmonth
echo ") s
echo "ON
echo "t1.zloaddate = s.zloaddate and
echo "t1.0comp_code=s.0comp_code and
echo "t1.0fiscper = s.0fiscper and
t1.0fiscvarnt = s.0fiscvarnt and
t1.0ac_doc_no = s.0ac_doc_no and
t1.0item_num = s.0item_num and
t1.0fi_dsbitem = s.0fi_dsbitem and
t1.0ven_compc = s.0ven_compc and
t1.0calmonth = s.0calmonth

