#!/bin/bash

$debug && set -x

function usage {

echo "extract_header.sh -s <source schema> -t <source tablename> -f <Header file name> -u <username> -p <password> -d <db_name> "
exit 1;
}

while getopts "s:t:f:u:p:c:d:h?" opt
do
case ${opt}  in
  s) schema=$OPTARG;;
  t) tab_name=$OPTARG;;
  f) header_file=$OPTARG;;
  u) username=$OPTARG;;
  p) password=$OPTARG;;
  c) connect_str=$OPTARG;;
  d) src_db_typ=$OPTARG;;
  *) usage;
     exit ;;

esac
done

typeset -u tab_name schema 

header_file=${header_file:-"${schema}.${tab_name}.txt"}

echo "Extracting header information for ${schema}.${tab_name}"

if [ ! -f ${HEADER_HOME}/${header_file} ]; then

  echo "Extracting Header Information for ${src_schema} ${src_tab_nm}"
  if [ "${src_db_typ?}" = "oracle" ]; then
    groovy ${SCRIPT_HOME}/oracle_connector.groovy2  ${username} $(echo ${password?}|${SCRIPT_HOME}/password -d) \
                                  ${connect_str} header ${schema} ${tab_name} \
                                  > ${HEADER_HOME}/${header_file}
  elif [ "${src_db_typ?}" = "sqlserver" ]; then
    groovy ${SCRIPT_HOME}/sqlserver_connector.groovy2 ${username} $(echo ${password?}|${SCRIPT_HOME}/password -d) \
                               ${connect_str} ${request_typ} header ${schema} ${tab_name} \
                               > ${HEADER_HOME}/${header_file}
  elif [ "${src_db_typ?}" = "db2" ]; then
    groovy ${SCRIPT_HOME}/db2_connector.groovy2 ${username} $(echo ${password?}|${SCRIPT_HOME}/password -d) \
                               ${connect_str} ${request_typ} header ${schema} ${tab_name} \
                               > ${HEADER_HOME}/${header_file}
  elif [ "${src_db_typ?}" = "mysql" ]; then
    groovy ${SCRIPT_HOME}/mysql_connector.groovy2 ${username} $(echo ${password?}|${SCRIPT_HOME}/password -d) \
                               ${connect_str} ${request_typ} header ${schema} ${tab_name} \
                               > ${HEADER_HOME}/${header_file}

  fi;

  if [ 0 -ne $? ]; then
    echo "Extract of header for ${schema}.${tab_name} Failed ....exiting"
    rm -f ${HEADER_HOME?}/${header_file}
    exit 1;
  fi;
else
  echo "Header file already exists in ${HEADER_HOME}/${header_file}"
fi;
