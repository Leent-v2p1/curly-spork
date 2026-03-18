export HADOOP_CONF_DIR=/etc/hive/conf

user_name=${1}
keytab=${2}
queue=${3}
principal=${4}
metastore_uri=${5}
hiveJdbcUrl=${6}
hive_sql=${7}

domain=$(echo $principal | grep -o "@.*")
userPrincipal="$user_name$domain"

echo "principal=$principal"
echo "userPrincipal=$userPrincipal"
echo "keytab=$keytab"
echo "queue=$queue"
echo "metastore_uri=$metastore_uri"
echo "hiveJdbcUrl=$hiveJdbcUrl"
echo "hive_sql=$hive_sql"

spark2-submit \
    --class ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.HiveScriptRunner \
    --executor-memory 2G \
    --executor-cores 1 \
    --num-executors 1 \
    --driver-memory 2G \
    --principal $userPrincipal \
    --keytab $keytab \
    --files custom-log4j.properties \
    --master yarn \
    --queue $queue \
    --deploy-mode client \
    --conf spark.driver.extraClassPath=personalization.jar:/opt/cloudera/parcels/CDH/jars/hive-jdbc-1.1.0-cdh5.16.2.jar:/opt/cloudera/parcels/CDH/jars/hive-service-1.1.0-cdh5.16.2.jar \
    --conf spark.extraListeners=ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.TaskInfoRecorderListener \
    --conf spark.dynamicAllocation.enabled=false \
    --conf "spark.driver.extraJavaOptions= \
        -Dhive.metastore.uris=$metastore_uri \
        -Dhive.metastore.sasl.enabled=true \
        -Dlog4j.configuration=file:./custom-log4j.properties \
        -Dspark.hiveJdbcUrl=$hiveJdbcUrl \
        -Dspark.hive.sql=$hive_sql \
" \
personalization.jar
