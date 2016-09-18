#!/bin/bash

$debug && set -x

export ts=$(date '+%Y%m%d.%H%M%S')

function usage {

echo "migrate_data.sh -s <source schema> -t <source tablename>  -n <servie name>"

}

#function logit() 
#{
#echo "[`date`] : ${2} - ${3}" >> ${LOG_HOME}/${1}.${ts}
#export LOG_FILE=${LOG_HOME}/${1}.${ts}
#}


function audit
{

  $debug && set -x
  typeset -u refresh_typ;

  extract_id=$1
  status=$2
  start_ts=$3
  tab_name=$4
  refresh_typ=$5
  fail_step=$6
  end_ts=$(date '+%s');


  ((elapse_ts=${end_ts}-${start_ts}));
  if [ "${status}" = "F" ]; then
    mysql -u${DB_USER} -p$(echo ${DB_PASS}|${SCRIPT_HOME}/password -d) -h${DB_HOST} -D dataload  -e "\
    insert into extract_audit \
      (EXEC_ID,EXTRACT_ID, STG_TAB_NM, REFRESH_TYP, EXEC_STRT_TS,EXEC_END_TS,EXEC_LAPSE_S,STATUS,FAIL_STEP) values \
      (${batch_id?},${extract_id?},'${tab_name?}','${refresh_typ}','$(date -d @${start_ts} +"%Y-%m-%d %T" )', \
      '$(date -d @${end_ts} +"%Y-%m-%d %T" )',${elapse_ts},'${status}',${fail_step})";
  else
    src_rows=$(grep "Map output records" ${LOG_FILE}|awk ' BEGIN {FS="="}{print $2}')
    tgt_rows=$(hive --silent -e "select count(*) from ${interface_nm}.${tab_name}")
    if [ ${src_rows?} -eq ${tgt_rows?} ]; then
      mysql -u${DB_USER} -p$(echo ${DB_PASS}|${SCRIPT_HOME}/password -d) -h${DB_HOST} -D dataload  -e "\
      insert into extract_audit \
        (EXEC_ID,EXTRACT_ID, STG_TAB_NM, SOURCE_ROW, REFRESH_TYP, TGT_ROWS,EXEC_STRT_TS,EXEC_END_TS,EXEC_LAPSE_S,STATUS) values \
        (${batch_id?},${extract_id?},'${tab_name?}',${src_rows},'${refresh_typ}',${tgt_rows}, \
        '$(date -d @${start_ts} +"%Y-%m-%d %T" )', '$(date -d @${end_ts} +"%Y-%m-%d %T" )',${elapse_ts},'${status}')";
    else
      mysql -u${DB_USER} -p$(echo ${DB_PASS}|${SCRIPT_HOME}/password -d) -h${DB_HOST} -D dataload  -e "\
      insert into extract_audit \
        (EXEC_ID,EXTRACT_ID, STG_TAB_NM, SOURCE_ROW, REFRESH_TYP, TGT_ROWS,EXEC_STRT_TS,EXEC_END_TS,EXEC_LAPSE_S,STATUS) values \
        (${batch_id?},${extract_id?},'${tab_name?}',${src_rows},'${refresh_typ}',${tgt_rows}, \
        '$(date -d @${start_ts} +"%Y-%m-%d %T" )', '$(date -d @${end_ts} +"%Y-%m-%d %T" )',${elapse_ts},'F')";
    fi;
  fi;

}

while getopts "s:t:n:d:r:l:?" opt
do
case ${opt}  in
  s) schema=$OPTARG;;
  t) tab_name=$OPTARG;;
  n) svc_name=$OPTARG;;
  d) hive_db=$OPTARG;;
  r) STEP=$OPTARG;;
  l) log_file=$OPTARG;;
  *) usage;
     exit ;;

esac
done


export STEP=${STEP:-"1"}
typeset -u tab_name schema svc_name interface_nm is_partition ;
typeset -l refresh_typ;
start_ts=$(date '+%s')
typeset -l source_db_typ;

mysql -u${DB_USER} -p$(echo ${DB_PASS}|${SCRIPT_HOME}/password -d) -h${DB_HOST} -D dataload --skip-column-names \
            -e "select CONCAT(a.SERVICE_NM,'|',a.SOURCE_DB_TYP,'|',a.HOSTNAME,'|', \
            a.PORT,'|',a.DB_NAME,'|',a.SOURCE_DB_TYP,'|',a.username,'|',a.password,'|', COALESCE(b.RAW_ARCHIVING,'NULL'),'|', \
            COALESCE(b.SRC_SCHEMA,'NULL'),'|', COALESCE(b.INTERFACE_NM,'NULL'),'|',COALESCE(b.REFRESH_TYP,'NULL') ,'|', COALESCE(b.EXTRACT_ID,'NULL') ,'|', \
            COALESCE(b.INCR_PK_COL,'NULL'),'|',COALESCE(b.INCR_COL_NM,'NULL'),'|', COALESCE(b.IS_PARTITION,'NULL'),'|', COALESCE(b.PARTITION_COL_NM,'NULL') ,'|', \
            COALESCE(b.STG_FILE_FORMAT,'NULL'),'|',COALESCE(b.IMPORT_FILE_FORMAT,'NULL'),'|',COALESCE(b.FILE_COMPRESS,'NULL')) \
            from extract_tab_config b, source_config a where a.service_nm=b.service_nm and  b.SRC_SCHEMA='${schema}' \
            and b.SRC_TAB_NM='${tab_name}' and a.SERVICE_NM = '${svc_name}' limit 1;"| \
            while IFS="|" read service_nm source_db_typ1 hostname port db_name source_db_typ username password raw_archiving\
            src_schema interface_nm refresh_typ extract_id incr_pk_col incr_col_nm is_partition   partition_col_nm stg_file_format \
            import_file_format file_compress


#mysql -u${DB_USER} -p$(echo ${DB_PASS}|${SCRIPT_HOME}/password -d) -h${DB_HOST} -D dataload --skip-column-names \
#            -e "select a.SERVICE_NM,a.SOURCE_DB_TYP,a.HOSTNAME, \
#            a.PORT,a.DB_NAME,a.SOURCE_DB_TYP,a.USERNAME,a.PASSWORD, \
#            b.SRC_SCHEMA,b.INTERFACE_NM, b.REFRESH_TYP  ,b.EXTRACT_ID, \
#            b.INCR_COL_NM, b.IS_PARTITION,b. PARTITION_COL_NM , b.STG_FILE_FORMAT, b.IMPORT_FILE_FORMAT,b.FILE_COMPRESS \
#            from extract_tab_config b, source_config a where a.service_nm=b.service_nm and  b.SRC_SCHEMA='${schema}' \
#            and b.SRC_TAB_NM='${tab_name}' and a.SERVICE_NM = '${svc_name}' limit 1" |\
#            while read "service_nm" "source_db_typ1" "hostname" "port" "db_name" "source_db_typ" "username" "password" \
#            "src_schema" "interface_nm" "refresh_typ" "extract_id" "incr_col_nm" "is_partition"   "partition_col_nm" "stg_file_format" \
#            "import_file_format" "file_compress"

do

   logit ${log_file} INFO "Start of Ingestion for Source ${src_schema}.${tab_name}"
   logit ${LOG_FILE} INFO "Parameters received :"
   logit ${LOG_FILE} INFO "SERVICE_NM :	${service_nm}"
   logit ${LOG_FILE} INFO "SRC_SCHEMA :	${src_schema}"
   logit ${LOG_FILE} INFO "SRC_TABLE_NAME:	${tab_name}"
   logit ${LOG_FILE} INFO "INTERFACE_NM :	${interface_nm}"
   logit ${LOG_FILE} INFO "REFRESH_TYPE :	${refresh_typ}"
   logit ${LOG_FILE} INFO "IS_PARTITION :	${is_partition}"

   if [ "${is_part}" = "Y" ]; then
     part_col_nm=${partition_col_nm}
   else
     part_col_nm="none"
   fi;


   if [ "${refresh_typ}" = "full" ]; then
     refresh_dir="full"
     sub_dir="1"
   elif [ "${refresh_typ}" = "incr" ]; then
     refresh_dir="incremental"
     sub_dir=""
     where_predicate=$(hive --silent -e "select max(${incr_col_nm}) from ${interface_nm}.${tab_name}")
   fi;


   RAW_FILE_PATH=${RAW_LANDING_PATH?}/${interface_nm?}/${tab_name?}/current
   STG_FILE_PATH=$(echo ${STAGING_PATH}/${interface_nm?}/${tab_name?}/${refresh_dir}/${sub_dir}|sed "s/\/\//\//g")


  
   if [[ ${STEP} -eq 1 ]]; then 
     if [ -d ${OUT_HOME?}/${src_schema}.${tab_name} ]; then
       logit ${LOG_FILE} INFO  "Removing existing intermediate data for ${src_schema}.${tab_name}"
       rm -rf ${OUT_HOME?}/${src_schema?}.${tab_name?} >> ${LOG_FILE} 2>&1
       if [ $? -ne 0 ]; then
         logit  ${LOG_FILE} WARNING "Failure to Remove Directory"
       fi;
     fi;

     logit ${LOG_FILE} INFO  "Starting Sqooping data from source system"
     ${SCRIPT_HOME}/sqoop_data.sh -s ${schema?} -t ${tab_name?} -n ${svc_name?} -h ${where_predicate:-"none"} >> ${LOG_FILE} 2>&1
     if [ $? -eq 0 ]; then
       logit ${LOG_FILE} INFO "Sqoop of data successful "
     else
       logit ${LOG_FILE} ERROR "Sqoop of data failed"
       audit "${extract_id?}" "F" "${start_ts?}" "${tab_name?}"  "${refresh_typ}" "${STEP}"
       exit 1;
     fi
  fi;
  if [[ ${STEP} -eq 1 || ${STEP} -eq 2 ]]; then
     logit ${LOG_FILE} INFO  "Starting file movement from sqoop location to raw location"
     ${SCRIPT_HOME}/sqoop2raw.sh -i ${interface_nm} -t ${tab_name} -p ${is_partition} -r ${refresh_typ} -s ${src_schema} -a ${raw_archiving} >> ${LOG_FILE} 2>&1
     if [ $? -eq 0 ]; then
       logit ${LOG_FILE} INFO "Sqoop data movement  successful "
     else
       logit ${LOG_FILE} ERROR "Sqoop data movement failed"
       audit "${extract_id?}" "F" "${start_ts?}" "${tab_name?}"  "${refresh_typ}" "${STEP}"
       exit 1;
     fi;
  fi;

  if [[ ${STEP} -eq 1 || ${STEP} -eq 2 || ${STEP} -eq 3 ]]; then
     logit ${LOG_FILE} INFO  "Start creation of raw table"
     ${SCRIPT_HOME}/create_raw_table.sh -s ${src_schema} -t ${tab_name} -d ${RAW_FILE_PATH?} -p none -r ${refresh_typ} \
                                        -i ${interface_nm}  -f ${import_file_format} -o ${source_db_typ} >> ${LOG_FILE} 2>&1
     if [ $? -eq 0 ]; then
       logit ${LOG_FILE} INFO "Raw table creation successful"
     else
       logit ${LOG_FILE} ERROR "Raw table creation failed"
       audit "${extract_id?}" "F" "${start_ts?}" "${tab_name?}"  "${refresh_typ}" "${STEP}"
       exit 1;
     fi;
  fi;
  if [[ (${STEP} -eq 1 || ${STEP} -eq 2 || ${STEP} -eq 3 || ${STEP} -eq 4) && ${ONLY_RAW_PROCESSING} -eq 0 ]]; then
     logit ${LOG_FILE} INFO  "Start creation of staging table"
     ${SCRIPT_HOME}/create_stg_table.sh -s ${src_schema} -t ${tab_name} -d ${STG_FILE_PATH?} -p ${part_col_nm} -r ${refresh_typ} \
                                        -i ${interface_nm} -f ${stg_file_format}  >> ${LOG_FILE} 2>&1
     if [ $? -eq 0 ]; then
       logit ${LOG_FILE} INFO "Staging table creation successful"
     else
       logit ${LOG_FILE} ERROR "Staging table creation failed"
       audit "${extract_id?}" "F" "${start_ts?}" "${tab_name?}"  "${refresh_typ}" "${STEP}"
       exit 1;
     fi;
  fi;
  if [[ (${STEP} -eq 1 || ${STEP} -eq 2 || ${STEP} -eq 3 || ${STEP} -eq 4 || ${STEP} -eq 5) && ${ONLY_RAW_PROCESSING} -eq 0 ]]; then
     logit ${LOG_FILE} INFO  "Starting to Transform data and move from Raw to Staging"
     ${SCRIPT_HOME}/transform_data.sh -s ${src_schema} -t ${tab_name} -p ${part_col_nm} -r ${refresh_typ} -i ${interface_nm}  >> ${LOG_FILE} 2>&1
     if [ $? -eq 0 ]; then
       logit ${LOG_FILE} INFO "Data Transformation Successul"
     else
       logit ${LOG_FILE} ERROR "Data Transformation Failed"
       audit "${extract_id?}" "F" "${start_ts?}" "${tab_name?}"  "${refresh_typ}" "${STEP}"
       exit 1;
     fi;
  fi;
  if [[ (${STEP} -eq 1 || ${STEP} -eq 2 || ${STEP} -eq 3 || ${STEP} -eq 4 || ${STEP} -eq 5 || ${STEP} -eq 6) && ${ONLY_RAW_PROCESSING} -eq 0 ]]; then
    if [[ "${refresh_typ?}" = "incr" ]]; then
       logit ${LOG_FILE} INFO  "Updating data for refresh type ${refresh_typ?} in ${interface_nm}.${tab_name}"
 
       if [ "${is_partition}" =  "Y" ]; then
         echo "Under construction"
       fi

       ${SCRIPT_HOME}/create_incr_update.sh -s ${src_schema} -t ${tab_name} -p "${part_col_nm}" -r ${refresh_typ} -h  "${src_schema}.${tab_name}.txt" \
                                            -i ${interface_nm} -s ${src_schema} -k "${incr_pk_col?}" >> ${LOG_FILE} 2>&1
       if [ $? -eq 0 ]; then
         logit ${LOG_FILE} INFO "Update on ${interface_nm}.${tab_name} Successful"
       else
         logit ${LOG_FILE} ERROR "Update on ${interface_nm}.${tab_name} Failed"
         audit "${extract_id?}" "F" "${start_ts?}" "${tab_name?}"  "${refresh_typ}" "${STEP}"
         exit 1;
       fi;
    elif [[ "${refresh_typ?}" = "append" ]]; then
      logit ${LOG_FILE} INFO  "Updating data for refresh type ${refresh_typ?} in ${interface_nm}.${tab_name}"

   
    fi;
#    if [[ (${STEP} -eq 1 || ${STEP} -eq 2 || ${STEP} -eq 3 || ${STEP} -eq 4 || ${STEP} -eq 5 || ${STEP} -eq 6 || ${STEP} -eq 7) && ${ONLY_RAW_PROCESSING} -eq 0 ]]; then
#       logit ${LOG_FILE} INFO  "Starting to Transform data and move from Raw to Staging"
#       ${SCRIPT_HOME}/update_data.sh -s ${src_schema} -t ${tab_name} -p ${part_col_nm} -r ${refresh_typ} -i ${interface_nm}  >> ${LOG_FILE} 2>&1
#       if [ $? -eq 0 ]; then
#         logit ${LOG_FILE} INFO "Data Transformation Successul"
#       else
#         logit ${LOG_FILE} ERROR "Data Transformation Failed"
#         audit "${extract_id?}" "F" "${start_ts?}" "${tab_name?}"  "${refresh_typ}" "${STEP}"
#         exit 1;
#       fi;
#    fi;
  fi;
audit "${extract_id?}" "S" "${start_ts?}" "${tab_name?}"  "${refresh_typ}" "${STEP}"
done

