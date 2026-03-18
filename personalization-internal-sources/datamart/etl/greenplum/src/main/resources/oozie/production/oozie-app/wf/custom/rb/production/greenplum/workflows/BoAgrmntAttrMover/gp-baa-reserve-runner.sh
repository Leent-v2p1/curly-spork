source gp-baa-reserve-properties-spark.conf

export HADOOP_CONF_DIR=/usr/sdp/current/hadoop-client/conf
export SPARK_CONF_DIR=/usr/sdp/current/spark3-client/conf
export YARN_CONF_DIR=/usr/sdp/current/hadoop-yarn-client/conf
export SPARK_MAJOR_VERSION=3

domain=$(echo $principal | grep -o "@.*")
userPrincipal="$user_name$domain"
keytab=$(echo $keytabPath | grep -o "\w*\.keytab")

echo "domain=$domain"
echo "userPrincipal=$userPrincipal"
echo "keytabPath=$keytabPath"
echo "keytab=$keytab"

spark-submit \
    --class ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.reserving.ReservingService \
    --properties-file gp-baa-reserve-properties-system.conf \
    --executor-memory $executorMemory \
    --executor-cores $executorCoreNum \
    --num-executors $executors \
    --driver-memory $driverMemory \
    --principal $userPrincipal \
    --keytab $keytab \
    --files custom-log4j.properties \
    --master yarn \
    --queue $queue \
    --deploy-mode client \
    --jars greenplum-datamart.jar,personalization.jar\
    --conf spark.driver.extraClassPath=greenplum-datamart.jar:personalization.jar:\
/opt/cloudera/parcels/CDH/lib/hive/lib/hive-jdbc.jar:/opt/cloudera/parcels/CDH/lib/hive/lib/hive-service.jar \
    --conf spark.extraListeners=ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.TaskInfoRecorderListener \
    --conf spark.dynamicAllocation.enabled=false \
    --conf "spark.driver.extraJavaOptions= \
        -Dhive.metastore.uris=$metastore_uri \
        -Dhive.metastore.sasl.enabled=true \
        -Dlog4j.configuration=file:./custom-log4j.properties \
" \
personalization.jar
