DROP TABLE IF EXISTS `extract_audit`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
) ;
