WITH daily_union AS (
    SELECT * FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl
    UNION ALL SELECT * FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl2
    UNION ALL SELECT * FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl3
    UNION ALL SELECT * FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl4
    UNION ALL SELECT * FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl5
    UNION ALL SELECT * FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl6
    UNION ALL SELECT * FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl7
    UNION ALL SELECT * FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl_gf
),
normalized AS (
    SELECT
        cast(epk_id AS bigint) AS epk_id,
        login_id,
        device_info,
        application,
        CASE WHEN upper(application) = 'PHIZ_SBER_ID' THEN 'INTERNET_BANK_SBERID' ELSE channel_type END AS channel_type,
        CASE
            WHEN channel_type IS NOT NULL THEN channel_type
            WHEN upper(application) LIKE '%MOBIL%' THEN 'MAPI'
            WHEN upper(application) = 'PHIZ_IC' THEN 'WEB'
            WHEN upper(application) LIKE '%ATM%' THEN 'ATM'
            WHEN upper(application) = 'PHIZ_IA' THEN 'SERVICE'
            ELSE NULL
        END AS channel_type_all,
        tb_nmb,
        osb_nmb,
        vsp_nmb,
        schema,
        logon_dt,
        cnt_logon,
        day_part
    FROM daily_union
    WHERE epk_id IS NOT NULL
      AND day_part < ${first_day_current_month}
      AND day_part >= ${load_start_dt}
      AND day_part < ${load_end_dt}
)
SELECT
    coalesce(epk_id, -1) AS epk_id,
    substr(day_part, 1, 7) AS month_part,
    min(logon_dt) AS fst_logon_dt,
    max(logon_dt) AS lst_logon_dt,
    sum(cnt_logon) AS tot_qty,
    min(CASE WHEN upper(channel_type_all) IN ('MAPI', 'MB') THEN logon_dt END) AS fst_mobil_dt,
    max(CASE WHEN upper(channel_type_all) IN ('MAPI', 'MB') THEN logon_dt END) AS lst_mobil_dt,
    sum(CASE WHEN upper(channel_type_all) IN ('MAPI', 'MB') THEN cnt_logon ELSE 0 END) AS mobil_qty,
    min(CASE WHEN upper(channel_type_all) IN ('WEB', 'CSA') THEN logon_dt END) AS fst_web_dt,
    max(CASE WHEN upper(channel_type_all) IN ('WEB', 'CSA') THEN logon_dt END) AS lst_web_dt,
    sum(CASE WHEN upper(channel_type_all) IN ('WEB', 'CSA') THEN cnt_logon ELSE 0 END) AS web_qty,
    min(CASE WHEN upper(channel_type_all) IN ('ATM', 'TERMINAL') THEN logon_dt END) AS fst_atm_dt,
    max(CASE WHEN upper(channel_type_all) IN ('ATM', 'TERMINAL') THEN logon_dt END) AS lst_atm_dt,
    sum(CASE WHEN upper(channel_type_all) IN ('ATM', 'TERMINAL') THEN cnt_logon ELSE 0 END) AS atm_qty,
    min(CASE WHEN upper(channel_type_all) = 'SERVICE' THEN logon_dt END) AS fst_arm_dt,
    max(CASE WHEN upper(channel_type_all) = 'SERVICE' THEN logon_dt END) AS lst_arm_dt,
    sum(CASE WHEN upper(channel_type_all) = 'SERVICE' THEN cnt_logon ELSE 0 END) AS arm_qty,
    min(CASE WHEN upper(channel_type_all) = 'INTERNET_BANK_SBERID' THEN logon_dt END) AS fst_sberid_dt,
    max(CASE WHEN upper(channel_type_all) = 'INTERNET_BANK_SBERID' THEN logon_dt END) AS lst_sberid_dt,
    sum(CASE WHEN upper(channel_type_all) = 'INTERNET_BANK_SBERID' THEN cnt_logon ELSE 0 END) AS sberid_qty,
    count(*) AS tot_day_qty,
    sum(CASE WHEN upper(channel_type_all) IN ('MAPI', 'MB') THEN 1 ELSE 0 END) AS mobil_day_qty,
    sum(CASE WHEN upper(channel_type_all) IN ('WEB', 'CSA') THEN 1 ELSE 0 END) AS web_day_qty,
    sum(CASE WHEN upper(channel_type_all) IN ('ATM', 'TERMINAL') THEN 1 ELSE 0 END) AS atm_day_qty,
    sum(CASE WHEN upper(channel_type_all) = 'SERVICE' THEN 1 ELSE 0 END) AS arm_day_qty,
    sum(CASE WHEN upper(channel_type_all) = 'INTERNET_BANK_SBERID' THEN 1 ELSE 0 END) AS sberid_day_qty
FROM normalized
GROUP BY coalesce(epk_id, -1), substr(day_part, 1, 7);
