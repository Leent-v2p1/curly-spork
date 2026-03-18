source gp-bjs-properties-spark.conf

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
    --class ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.BO.BoJeSecurity \
    --properties-file gp-bjs-properties-system.conf \
    --executor-memory $executorMemory \
    --executor-cores $executorCoreNum \
    --num-executors $executors \
    --driver-memory $driverMemory \
    --principal $userPrincipal \
    --keytab $keytab \
    --driver-class-path greenplum-connector-apache-spark-scala_2.12-2.1.3.jar \
    --files "custom-log4j.properties#custom-log4j.properties,$gpJaas#$gpJaas,$gpKeytab#$gpKeytab" \
    --master yarn \
    --queue $queue \
    --deploy-mode client \
    --jars greenplum-datamart.jar,personalization.jar,postgresql-42.0.0.jre6.jar,greenplum-connector-apache-spark-scala_2.12-2.1.3.jar\
    --conf spark.driver.extraClassPath=greenplum-datamart.jar:personalization.jar:\
postgresql-42.0.0.jre6.jar:greenplum-connector-apache-spark-scala_2.12-2.1.3.jar \
    --conf spark.executor.extraClassPath=greenplum-datamart.jar:personalization.jar:\
postgresql-42.0.0.jre6.jar:greenplum-connector-apache-spark-scala_2.12-2.1.3.jar \
    --conf spark.extraListeners=ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.TaskInfoRecorderListener \
    --conf spark.dynamicAllocation.enabled=false \
    --conf "spark.driver.extraJavaOptions= \
        -Dhive.metastore.uris=$metastore_uri \
        -Dhive.metastore.sasl.enabled=true \
        -Djava.security.auth.login.config=./$gpJaas \
        -Dlog4j.configuration=file:./custom-log4j.properties \
" \
    --conf "spark.executor.extraJavaOptions= \
        -Djava.security.auth.login.config=./$gpJaas \
" \
greenplum-datamart.jar