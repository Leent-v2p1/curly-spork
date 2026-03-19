-- Эталонный SQL-прототип для семейства sbol_logon_aggr_dt_*.
-- Параметры:
--   ${src_schema}      - схема исходных данных
--   ${source_table}    - одна из витрин sbol_logon_ikfl*
--   ${login_table}     - таблица logins соответствующего инстанса
--   ${target_table}    - целевая витрина sbol_logon_aggr_dt_*
--   ${instance_schema} - строковый код инстанса для колонки schema

WITH logins AS (
    SELECT
        id,
        last_logon_card_tb  AS tb_nmb,
        last_logon_card_osb AS osb_nmb,
        last_logon_card_vsp AS vsp_nmb
    FROM ${src_schema}.${login_table}
),
legacy_logons AS (
    SELECT
        cast(src.login_id AS decimal(38, 0)) AS login_id,
        cast(src.application AS string)      AS application,
        cast(src.channel_type AS string)     AS channel_type,
        cast(src.logon_dt AS timestamp)      AS logon_dt,
        cast(src.epk_id AS bigint)           AS epk_id,
        cast(src.device_info AS string)      AS device_info,
        logins.tb_nmb,
        logins.osb_nmb,
        logins.vsp_nmb,
        cast(src.day_part AS string)         AS day_part
    FROM ${src_schema}.${source_table} src
    LEFT JOIN logins ON cast(src.login_id AS decimal(38, 0)) = cast(logins.id AS decimal(38, 0))
    WHERE src.logon_dt >= ${start_ts}
      AND src.logon_dt < ${end_ts}
      AND src.logon_dt < date_trunc('DAY', current_timestamp())
      AND src.day_part >= ${start_dt}
      AND (
            src.day_part < ${migration_dt}
         OR (src.day_part >= ${migration_dt} AND upper(coalesce(src.channel_type, '')) IN ('MAPI', 'WEB', 'SBOL'))
      )
),
launcher_logons AS (
    SELECT
        cast(logon_id AS decimal(38, 0)) AS login_id,
        cast(NULL AS string)             AS application,
        cast(channel_type AS string)     AS channel_type,
        cast(logon_dt AS timestamp)      AS logon_dt,
        cast(epk_id AS bigint)           AS epk_id,
        cast(device_info AS string)      AS device_info,
        cast(tb_nmb AS string)           AS tb_nmb,
        cast(osb_nmb AS string)          AS osb_nmb,
        cast(vsp_nmb AS string)          AS vsp_nmb,
        cast(substr(logon_dt, 1, 10) AS string) AS day_part
    FROM ${src_schema}.sbol_logon_launcher
    WHERE upper(type) = 'LOGON'
      AND ${instance_filter}
      AND day_part >= ${migration_dt}
      AND day_part >= ${start_dt}
      AND logon_dt >= ${start_ts}
      AND logon_dt < ${end_ts}
      AND logon_dt < date_trunc('DAY', current_timestamp())
),
all_logons AS (
    SELECT * FROM legacy_logons
    UNION ALL
    SELECT * FROM launcher_logons
)
SELECT
    login_id,
    application,
    channel_type,
    cast(date_trunc('DAY', logon_dt) AS timestamp) AS logon_dt,
    coalesce(cast(epk_id AS bigint), -1)           AS epk_id,
    device_info,
    min(logon_dt)                                  AS min_logon_dt,
    max(logon_dt)                                  AS max_logon_dt,
    count(*)                                       AS cnt_logon,
    max(tb_nmb)                                    AS tb_nmb,
    max(osb_nmb)                                   AS osb_nmb,
    max(vsp_nmb)                                   AS vsp_nmb,
    ${instance_schema}                             AS schema,
    date_format(date_trunc('DAY', logon_dt), 'yyyy-MM-dd') AS day_part
FROM all_logons
GROUP BY
    login_id,
    application,
    channel_type,
    cast(date_trunc('DAY', logon_dt) AS timestamp),
    device_info,
    coalesce(cast(epk_id AS bigint), -1);
