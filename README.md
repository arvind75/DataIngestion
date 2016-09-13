# DataIngestion
The project is about automatic data ingestion from RDBMS into Hadoop and create hive tables without the need of porting DDLs

The project follows a parameter driven design where all the sources and objects are stores into a DBMS from where the script will pick the tables to migrate into HIVE.
The program is written in shell script and has a character based interactive screen.



Before we start executing the project, the following is required

1) Linux Machine with Bash Shell
2) MySQL installation
3) Create dataload databases
4) Execute the attached DDL to create the metadata tables for the project
5) Modify the .env file that stores password for the mysql instance
6) Keep in mind that the password is encrypted and you need to create an encrypted password with the password script
5) execute the "Ingest" program



MetaData Tables

DROP TABLE IF EXISTS `extract_audit`;
CREATE TABLE `extract_audit` (
  `EXEC_ID` bigint(20) DEFAULT NULL,
  `EXTRACT_ID` int(11) DEFAULT NULL,
  `STG_TAB_NM` varchar(100) DEFAULT NULL,
  `SOURCE_ROW` varchar(100) DEFAULT NULL,
  `REFRESH_TYP` varchar(100) DEFAULT NULL,
  `TGT_ROWS` int(11) DEFAULT NULL,
  `INCR_COL_VAL` varchar(100) DEFAULT NULL,
  `EXEC_STRT_TS` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `EXEC_END_TS` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `EXEC_LAPSE_S` int(11) DEFAULT NULL,
  `FAIL_STEP` int(11) DEFAULT NULL,
  `STATUS` varchar(100) DEFAULT NULL
);

DROP TABLE IF EXISTS `extract_tab_config`;
CREATE TABLE `extract_tab_config` (
  `EXTRACT_ID` int(11) DEFAULT NULL,
  `EXTRACT_NAME` varchar(100) DEFAULT NULL,
  `CREATE_JOB` char(1) DEFAULT 'N',
  `SRC_SCHEMA` varchar(100) DEFAULT NULL,
  `SRC_TAB_NM` varchar(100) DEFAULT NULL,
  `INTERFACE_NM` varchar(100) DEFAULT NULL,
  `IS_RAW` char(1) DEFAULT 'Y',
  `RAW_TAB_NM` varchar(100) DEFAULT NULL,
  `RAW_FILE_DIR` varchar(300) DEFAULT NULL,
  `RAW_ARCHIVING` char(1) DEFAULT 'N' COMMENT 'Applicable for REFRESH_TYP as INCR/APPEND when older RAW data is required to archive for future processing',
  `STG_TAB_NM` varchar(100) DEFAULT NULL,
  `STG_FILE_DIR` varchar(300) DEFAULT NULL,
  `REFRESH_TYP` varchar(10) DEFAULT 'FULL' COMMENT 'FULL / APPEND /INCR',
  `INCR_PK_COL` varchar(100) DEFAULT NULL COMMENT 'Primary Key column for Incremental tables for updates. In case of Composite columns, they should be entereed separated by space',
  `INCR_TAB_NM` varchar(100) DEFAULT NULL,
  `INCR_FILE_DIR` varchar(300) DEFAULT NULL,
  `INCR_COL_NM` varchar(100) DEFAULT NULL COMMENT 'Incremental updates with REFRESH_TYP as INCR/APPEND should have this column populated to extract data from source tables.',
  `INCR_COL_EQUATION` char(2) DEFAULT NULL,
  `IS_PARTITION` char(1) DEFAULT NULL,
  `PARTITION_COL_NM` varchar(100) DEFAULT NULL,
  `PARALLEL_EXTRACT` char(1) DEFAULT 'N',
  `EXTRACT_PARALLELISM` int(11) DEFAULT '1',
  `PARALLEL_SPLITBY` varchar(100) DEFAULT NULL,
  `QUERY` varchar(500) DEFAULT NULL,
  `MAP_COLUMNS` varchar(200) DEFAULT NULL,
  `IMPORT_FILE_FORMAT` varchar(10) DEFAULT 'TEXTFILE',
  `STG_FILE_FORMAT` varchar(10) DEFAULT 'PARQUET',
  `FILE_COMPRESS` varchar(1) DEFAULT 'Y',
  `SERVICE_NM` char(100) DEFAULT NULL,
  `CREATE_DT` date DEFAULT NULL,
  `MODIFY_DT` date DEFAULT NULL,
  `COMMENTS` varchar(300) DEFAULT NULL
);


DROP TABLE IF EXISTS `source_config`;
CREATE TABLE `source_config` (
  `SERVICE_NM` varchar(100) DEFAULT NULL,
  `SOURCE_DB_TYP` varchar(100) DEFAULT NULL,
  `HOSTNAME` varchar(100) DEFAULT NULL,
  `PORT` int(11) DEFAULT NULL,
  `DB_NAME` varchar(100) DEFAULT NULL,
  `CONNECT_TYP` varchar(100) DEFAULT NULL,
  `username` varchar(100) DEFAULT NULL,
  `password` varchar(100) DEFAULT NULL,
  `comments` varchar(200) DEFAULT NULL
)
