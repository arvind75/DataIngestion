#!/bin/bash

$debug && set -x

function usage  {
echo "Usage : "
exit 1;
}
while getopts "s:i:t:p:r:a:?" opt
do
case ${opt}  in
  s) src_schema=$OPTARG;;
  t) tablename=$OPTARG;;
  p) is_part=$OPTARG;;
  i) interface=$OPTARG;;
  r) refresh_type=$OPTARG;;
  a) raw_archiving=$OPTARG;;
  *) usage;
     exit ;;

esac
done

#batch_id=$(date '+%Y%m%d%H%M%S')  #Declared in .env
src_schema=${src_schema:-${interface}}
raw_archiving=${raw_archiving:-"N"}

typeset -u is_part interface ;
typeset -l refresh_type


if [ "${refresh_type}" = "full" ]; then
  refresh_dir="full"
  sub_dir="1"
elif [ "${refresh_type}" = "incr" ]; then
  refresh_dir="incremental"
  sub_dir=""
fi;


export SQOOP_FILE_PATH=${SQOOP_LANDING_PATH?}/${src_schema?}/${tablename?}
export RAW_FILE_PATH=${RAW_LANDING_PATH?}/${interface?}/${tablename?}/current
export RAW_ARCHIVE_PATH=${RAW_LANDING_PATH?}/${interface?}/${tablename?}/archive
export STG_FILE_PATH=${STAGING_PATH}/${interface?}/${tablename?}/${refresh_dir}/${sub_dir}


if [ "${raw_archiving?}" = "Y" ]; then
  ${SCRIPT_HOME?}/hfs -mkdir -p ${RAW_ARCHIVE_PATH}
  ${SCRIPT_HOME?}/hfs -mv ${RAW_FILE_PATH?}/* ${RAW_ARCHIVE_PATH}
fi


${SCRIPT_HOME?}/hfs -rm -r -f -skipTrash ${STG_FILE_PATH?}
${SCRIPT_HOME?}/hfs -rm -r -f -skipTrash ${RAW_FILE_PATH?}

${SCRIPT_HOME?}/hfs -mkdir -p ${RAW_FILE_PATH?}
${SCRIPT_HOME?}/hfs -mkdir -p ${STG_FILE_PATH?}

if [ "N" = "${is_part}" ]; then
  for file in $(${SCRIPT_HOME?}/hls ${SQOOP_FILE_PATH?}/*|awk '{print $8}')
  do
    filename=$(echo ${file}|awk ' BEGIN {FS="/"}{print $NF}')
    ${SCRIPT_HOME?}/hfs -cp ${SQOOP_FILE_PATH?}/${filename} ${RAW_FILE_PATH?}/${filename}.${batch_id}
  done
elif [ "Y" = "${is_part}" ]; then
  for file in $(${SCRIPT_HOME?}/hls "${SQOOP_FILE_PATH?}/*/*"|awk '{print $8}')
  do
    partition=$(echo ${file?}|awk ' BEGIN {FS="/"}{print $(NF-1)}')
    tgt_partition=$(echo ${partition?}|sed "s/=/_/g")
    filename=$(echo ${file?}|awk ' BEGIN {FS="/"}{print $NF}')
    ${SCRIPT_HOME}/hfs -cp ${SQOOP_FILE_PATH}/${partition?}/${filename?}  \
                           ${RAW_FILE_PATH}/${filename?}.${tgt_partition?}.${batch_id?}
 done
fi;
