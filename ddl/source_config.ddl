DROP TABLE IF EXISTS `source_config`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
) ;
