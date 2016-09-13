# DataIngestion
The project is about automatic data ingestion from RDBMS into Hadoop and create hive tables without the need of porting DDLs

The project follows a parameter driven design where all the sources and objects are stores into a DBMS from where the script will pick the tables to migrate into HIVE.
The program is written in shell script and has a character based interactive screen.



##Before we start executing the project, the following is required

1) Linux Machine with Bash Shell

2) MySQL installation

3) Create dataload databases

4) Execute the attached DDL to create the metadata tables for the project

5) Modify the .env file that stores password for the mysql instance

6) Keep in mind that the password is encrypted and you need to create an encrypted password with the password script

5) execute the "Ingest" program


##Currently Supported RDBMS

Oracle

SQL-SERVER


##Future RDBMS  Enhancement

DB2 UDB

SAP HANA

TERADATA


The utility can bring the data into a Raw area that keeps that data in the same state as the source. The data can then be moved to a STG (Staging) area where you can apply HIVE UDF transformation on the data for consumtion purposes. 

##Terms
1) INTERFACE_NM This is the schema/database name that you want to use for placing the data from different source databases into HIVE.

2) STG_FILE_FORMAT Staging file format for storage. Valid values examples are TEXTFILE, PARQUET, ORC, RC, AVRO

3) RAW_FILE_FORMAT RAW file format for storage. Valid values examples are TEXTFILE, PARQUET, ORC, RC. I recommned you have this in AVRO so that the data types are ported to HIVE.

4) FILE_COMPRESS  Compression technique used on the file format. Keep in mind that some of the fileformats have inbuilt compression like PARQUET uses snappy inbuilt.

5) CREATE_JOB This is a future enahancement that could create a job/script for each data extraction/refresh. This way, you can schedule these jobs via an external enterprise scheduler.

6) REFRESH_TYP This can have a value of FULL or INCR. The INCR implementation is a future enhancement to implement undate strategy.

7) KRB_EBNABLED Support Kerberos enabled cluster. Future enhancement to implement the program to support secured clusters.

8) ONLY_RAW_PROCESSING You have option to only extract data into the RAW area and not to process it further into Staging, based upon your business needs.

9) STAGING_TBL_PROPERTIES You have the ability to set additional table properties based upon data and business needs

10) SQOOP_LANDING_PATH Landing path of sqoop, Raw and Staging areas are different. This is done so that proper archival of incoming data can be done in your raw area by date. This required in scenario of recovery where you need to process data from prevoius sqoops.

11) DATA_DEL Delimiter for files in case using TEXTFILE format.

12) HIVE_AUX_JARS_PATH Path for your UDFs jars.

13) DBMS_TYP Default Database type from where ingestion happens.
