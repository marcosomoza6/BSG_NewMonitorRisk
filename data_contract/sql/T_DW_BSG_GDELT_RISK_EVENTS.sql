CREATE TABLE IF NOT EXISTS `new-risk-monitor.BSG_DS_NMR.T_DW_BSG_GDELT_RISK_EVENTS` (DTE_EVENT         DATE,
                                                                                     NAM_COUNTRY       STRING,
                                                                                     NAM_CITY          STRING,
                                                                                     CNT_EVENTS        INT64 NOT NULL,
                                                                                     NUM_GOLDSTEIN_AVG FLOAT64,
                                                                                     NUM_RISK_SCORE    FLOAT64,
                                                                                     DTE_INGESTION      DATE)
PARTITION BY DTE_EVENT
CLUSTER BY NAM_COUNTRY, NAM_CITY;
