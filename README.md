# DataIngestion
The project is about automatic data ingestion from RDBMS into Hadoop and create hive tables without the need of porting DDLs

The project follows a parameter driven design where all the sources and objects are stores into a DBMS from where the script will pick the tables to migrate into HIVE.
The program is written in shell script and has a character based interactive screen.



Before we start executing the project, the following is required

1) Linux Machine with Bash Shell
2) MySQL installation
3) Create dataload databases
4) Execute the attached DDL to create the metadata tables for the project
5) execute the "Ingest" program
