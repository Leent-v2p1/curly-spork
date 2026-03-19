WITH oper_union AS (
    SELECT * FROM ${src_schema}.sbol_oper_ikfl
    UNION ALL SELECT * FROM ${src_schema}.sbol_oper_ikfl2
    UNION ALL SELECT * FROM ${src_schema}.sbol_oper_ikfl3
    UNION ALL SELECT * FROM ${src_schema}.sbol_oper_ikfl4
    UNION ALL SELECT * FROM ${src_schema}.sbol_oper_ikfl5
    UNION ALL SELECT * FROM ${src_schema}.sbol_oper_ikfl6
    UNION ALL SELECT * FROM ${src_schema}.sbol_oper_ikfl7
    UNION ALL SELECT * FROM ${src_schema}.sbol_oper_ikfl_gf
),
filtered AS (
    SELECT *
    FROM oper_union
    WHERE epk_id IS NOT NULL
      AND oper_state_code = 'EXECUTED'
      AND oper_amt > 0
      AND day_part >= ${load_start_dt}
      AND day_part < ${first_day_current_month}
)
SELECT
    coalesce(cast(epk_id AS bigint), -1) AS epk_id,
    nullif(coalesce(login_type, ''), '') AS login_type,
    substr(date_create, 1, 7) AS month_part,
    min(date_create) AS fst_exec_dt,
    max(date_create) AS lst_exec_dt,
    cast(sum(oper_rur_amt) AS decimal(38, 15)) AS exec_rub_total,
    count(*) AS exec_qty,
    cast(sum(CASE WHEN login_type = 'CSA' THEN oper_rur_amt ELSE 0 END) AS decimal(38, 15)) AS exec_rub_web,
    cast(sum(CASE WHEN login_type = 'MAPI' THEN oper_rur_amt ELSE 0 END) AS decimal(38, 15)) AS exec_rub_mobil,
    cast(sum(CASE WHEN login_type = 'TERMINAL' THEN oper_rur_amt ELSE 0 END) AS decimal(38, 15)) AS exec_rub_atm,
    cast(sum(CASE WHEN login_type = 'MB' THEN oper_rur_amt ELSE 0 END) AS decimal(38, 15)) AS exec_rub_mb,
    cast(sum(CASE WHEN coalesce(login_type, '') NOT IN ('CSA', 'MAPI', 'TERMINAL', 'MB') THEN oper_rur_amt ELSE 0 END) AS decimal(38, 15)) AS exec_rub_other,
    sum(CASE WHEN login_type = 'CSA' THEN 1 ELSE 0 END) AS exec_web_qty,
    sum(CASE WHEN login_type = 'MAPI' THEN 1 ELSE 0 END) AS exec_mobil_qty,
    sum(CASE WHEN login_type = 'TERMINAL' THEN 1 ELSE 0 END) AS exec_atm_qty,
    sum(CASE WHEN login_type = 'MB' THEN 1 ELSE 0 END) AS exec_mb_qty,
    sum(CASE WHEN coalesce(login_type, '') NOT IN ('CSA', 'MAPI', 'TERMINAL', 'MB') THEN 1 ELSE 0 END) AS exec_other_qty
FROM filtered
GROUP BY coalesce(cast(epk_id AS bigint), -1), nullif(coalesce(login_type, ''), ''), substr(date_create, 1, 7);
