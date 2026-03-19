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
        CASE WHEN upper(application) = 'PHIZ_SBER_ID' THEN 'INTERNET_BANK_SBERID' ELSE channel_type END AS channel_type_norm,
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
        logon_dt
    FROM daily_union
    WHERE epk_id IS NOT NULL
      AND cast(epk_id AS bigint) <> -1
      AND day_part < ${current_day}
),
ever_dates AS (
    SELECT
        epk_id,
        min(logon_dt) AS fst_logon_ever_dt,
        max(logon_dt) AS lst_logon_ever_dt,
        min(CASE WHEN upper(channel_type_all) IN ('MAPI', 'MB') THEN logon_dt END) AS fst_mobil_ever_dt,
        max(CASE WHEN upper(channel_type_all) IN ('MAPI', 'MB') THEN logon_dt END) AS lst_mobil_ever_dt,
        min(CASE WHEN upper(channel_type_all) IN ('WEB', 'CSA') THEN logon_dt END) AS fst_web_ever_dt,
        max(CASE WHEN upper(channel_type_all) IN ('WEB', 'CSA') THEN logon_dt END) AS lst_web_ever_dt,
        min(CASE WHEN upper(channel_type_all) IN ('ATM', 'TERMINAL') THEN logon_dt END) AS fst_atm_ever_dt,
        max(CASE WHEN upper(channel_type_all) IN ('ATM', 'TERMINAL') THEN logon_dt END) AS lst_atm_ever_dt,
        min(CASE WHEN upper(channel_type_all) = 'SERVICE' THEN logon_dt END) AS fst_arm_ever_dt,
        max(CASE WHEN upper(channel_type_all) = 'SERVICE' THEN logon_dt END) AS lst_arm_ever_dt,
        min(CASE WHEN upper(channel_type_all) = 'INTERNET_BANK_SBERID' THEN logon_dt END) AS fst_sberid_ever_dt,
        max(CASE WHEN upper(channel_type_all) = 'INTERNET_BANK_SBERID' THEN logon_dt END) AS lst_sberid_ever_dt
    FROM normalized
    GROUP BY epk_id
),
last_logon AS (
    SELECT *
    FROM (
        SELECT
            epk_id,
            login_id,
            device_info,
            application,
            channel_type_norm AS channel_type,
            tb_nmb,
            osb_nmb,
            vsp_nmb,
            schema,
            row_number() OVER (PARTITION BY epk_id ORDER BY logon_dt DESC) AS rn
        FROM normalized
    ) src
    WHERE rn = 1
)
SELECT
    ever_dates.*,
    last_logon.login_id,
    last_logon.device_info,
    last_logon.application,
    last_logon.channel_type,
    last_logon.tb_nmb,
    last_logon.osb_nmb,
    last_logon.vsp_nmb,
    last_logon.schema
FROM ever_dates
LEFT JOIN last_logon USING (epk_id);
