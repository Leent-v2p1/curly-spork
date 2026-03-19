### Миграция ERIB-витрин на B2C-SQL

Папка содержит SQL-реализацию витрин из `personalization-internal-sources/datamart/etl/erib` по аналогии с `custom_rb_vnv` и `custom_rb_evk`.

В состав миграции включены:
- `sbol_logon_aggr_dt_*` для всех инстансов IKFL;
- `sbol_logon_aggr`;
- `sbol_logon_aggr_for_mega`;
- `sbol_logon_hist`;
- `sbol_oper_aggr`;
- `sbol_oper_aggr_for_mega`.

Реализация сохраняет структуру нового фреймворка: `etl/ddl`, `etl/workflows`, `resources/*`.
