#!/bin/bash


function usage  {

echo "create_incr_view.sh -s <source schema> -t <source tablename> -h <header file name>"
exit 1;
}

while getopts "s:t:k:?" opt
do
case ${opt}  in
  s) schema=$OPTARG;;
  t) tab_name=$OPTARG;;
  k) pk=$OPTARG;;
  *) usage;
     exit ;;

esac
done

typeset -u tab_name schema svc_name file_compress import_file_format ;
typeset -l partition;

for column in ${pk?}
do
 pk_column1=$(echo ${column},${pk_column1})
 where_clause=$(echo "t1.${column} = s.${column} and ${where_clause}")
done

pk_column=$(echo ${pk_column1}|awk '{print substr($0,1,length($0)-1)}')

default_time=$(date '+%Y-%m-%d %H:%M:%S')

echo "use ${schema?};"

cat << DELIM > $TMP_HOME/${schema}.${tab_name}_view.ddl
CREATE VIEW ${tab_name?}_view  AS 
SELECT t1.* FROM 
(SELECT * FROM ${tab_name}     
UNION ALL    
SELECT * FROM ${tab_name}_incr) t1 
JOIN
(SELECT ${pk_column?}, max(extract_ts) extract_ts 
FROM         
(SELECT ${pk_column?},extract_ts  FROM ${tab_name?}         
UNION ALL        
SELECT ${pk_column?},extract_ts FROM ${tab_name?}_incr) t2      
GROUP BY ${pk?}
) s  
ON 
${where_clause?}
t1.extract_ts = s.extract_ts 
;
DELIM

echo hive -v -f ${TMP_HOME?}/${schema}.${tab_name}_view.ddl
