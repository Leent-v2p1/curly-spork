WITH monthly AS (
    SELECT
        coalesce(cast(epk_id AS bigint), -1) AS epk_id,
        month_part,
        fst_logon_dt AS sbol_fst_login_dt,
        fst_atm_dt AS sbol_atm_fst_login_dt,
        fst_mobil_dt AS sbol_mob_fst_login_dt,
        fst_web_dt AS sbol_web_fst_login_dt,
        lst_logon_dt AS sbol_lst_login_dt,
        lst_atm_dt AS sbol_atm_lst_login_dt,
        lst_mobil_dt AS sbol_mob_lst_login_dt,
        lst_web_dt AS sbol_web_lst_login_dt,
        CASE WHEN tot_qty > 0 THEN 1 ELSE 0 END AS sbol_1m_login_flag,
        CASE WHEN atm_qty > 0 THEN 1 ELSE 0 END AS sbol_atm_1m_login_flag,
        CASE WHEN mobil_qty > 0 THEN 1 ELSE 0 END AS sbol_mob_1m_login_flag,
        CASE WHEN web_qty > 0 THEN 1 ELSE 0 END AS sbol_web_1m_login_flag,
        CASE WHEN sum(tot_qty) OVER (PARTITION BY epk_id ORDER BY month_part ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) > 0 THEN 1 ELSE 0 END AS sbol_3m_login_flag,
        CASE WHEN sum(atm_qty) OVER (PARTITION BY epk_id ORDER BY month_part ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) > 0 THEN 1 ELSE 0 END AS sbol_atm_3m_login_flag,
        CASE WHEN sum(mobil_qty) OVER (PARTITION BY epk_id ORDER BY month_part ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) > 0 THEN 1 ELSE 0 END AS sbol_mob_3m_login_flag,
        CASE WHEN sum(web_qty) OVER (PARTITION BY epk_id ORDER BY month_part ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) > 0 THEN 1 ELSE 0 END AS sbol_web_3m_login_flag
    FROM custom_rb_sbol.sbol_logon_aggr
    WHERE month_part < ${current_month}
),
with_ever AS (
    SELECT
        *,
        cast(min(sbol_fst_login_dt) OVER (PARTITION BY epk_id ORDER BY month_part ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS string) AS sbol_fst_login_ever_dt,
        cast(min(sbol_atm_fst_login_dt) OVER (PARTITION BY epk_id ORDER BY month_part ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS string) AS sbol_atm_fst_login_ever_dt,
        cast(min(sbol_mob_fst_login_dt) OVER (PARTITION BY epk_id ORDER BY month_part ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS string) AS sbol_mob_fst_login_ever_dt,
        cast(min(sbol_web_fst_login_dt) OVER (PARTITION BY epk_id ORDER BY month_part ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS string) AS sbol_web_fst_login_ever_dt,
        cast(max(sbol_lst_login_dt) OVER (PARTITION BY epk_id ORDER BY month_part ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS string) AS sbol_lst_login_ever_dt,
        cast(max(sbol_atm_lst_login_dt) OVER (PARTITION BY epk_id ORDER BY month_part ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS string) AS sbol_atm_lst_login_ever_dt,
        cast(max(sbol_mob_lst_login_dt) OVER (PARTITION BY epk_id ORDER BY month_part ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS string) AS sbol_mob_lst_login_ever_dt,
        cast(max(sbol_web_lst_login_dt) OVER (PARTITION BY epk_id ORDER BY month_part ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS string) AS sbol_web_lst_login_ever_dt
    FROM monthly
)
SELECT
    *,
    months_between(date_add(add_months(concat(month_part, '-01'), 1), -1), cast(sbol_fst_login_ever_dt AS timestamp)) AS sbol_mnth_fst_dt_qty,
    months_between(date_add(add_months(concat(month_part, '-01'), 1), -1), cast(sbol_lst_login_ever_dt AS timestamp)) AS sbol_mnth_lst_dt_qty,
    months_between(date_add(add_months(concat(month_part, '-01'), 1), -1), cast(sbol_atm_lst_login_ever_dt AS timestamp)) AS sbol_mnth_atm_lst_dt_qty,
    months_between(date_add(add_months(concat(month_part, '-01'), 1), -1), cast(sbol_mob_lst_login_ever_dt AS timestamp)) AS sbol_mnth_mob_lst_dt_qty,
    months_between(date_add(add_months(concat(month_part, '-01'), 1), -1), cast(sbol_web_lst_login_ever_dt AS timestamp)) AS sbol_mnth_web_lst_dt_qty
FROM with_ever;
