hdfs dfs -mkdir /data/custom/rb/triggers/pa/oakb_triggers_name/
hdfs dfs -cp /data/custom/rb/triggers/pa/oakb_triggers_name.csv /data/custom/rb/triggers/pa/oakb_triggers_name/
hive -e "create external table custom_rb_triggers.oakb_triggers_name (
trigger_code string,
trigger_name string)
row format delimited
fields terminated by '\u0059'
stored as textfile
location '/data/custom/rb/triggers/pa/oakb_triggers_name/'
tblproperties ('skip.header.line.count'='1')"
