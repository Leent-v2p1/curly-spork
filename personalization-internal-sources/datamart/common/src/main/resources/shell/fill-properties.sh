export HADOOP_CONF_DIR=/usr/sdp/current/hadoop-client/conf
export SPARK_CONF_DIR=/usr/sdp/current/spark3-client/conf
export YARN_CONF_DIR=/usr/sdp/current/hadoop-yarn-client/
export SPARK_MAJOR_VERSION=3

ctlUrl=${1}
loadingId=${2}
wfId=${3}
loadingStart=${4}
principal=${5}
user_name=${6}
keytab=${7}
queue=${8}
customBuildDate=${9}
propertyPath=${10}
systemPropertyPath=${11}
ctlApiVersionV5=${12}

domain=$(echo $principal | grep -o "@.*")
userPrincipal="$user_name$domain"

echo "spark.ctl.url=$ctlUrl"
echo "spark.ctl.loading.id=$loadingId"
echo "spark.ctl.wf.id=$wfId"
echo "spark.ctl.loading.start=$loadingStart"
echo "spark.ctl.api.version.v5=$ctlApiVersionV5"
echo "principal=$principal"
echo "keytab=$keytab"
echo "queue=$queue"
echo "propertyPath=$propertyPath"
echo "systemPropertyPath=$systemPropertyPath"
echo "spark.custom.build.date=$customBuildDate"

spark-submit \
    --class ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.ConfigGenerator \
    --principal $userPrincipal \
    --keytab $keytab \
    --files custom-log4j.properties \
    --master yarn \
    --queue $queue \
    --deploy-mode client \
    --conf spark.driver.extraClassPath=personalization.jar \
    --conf "spark.driver.extraJavaOptions= \
        -Dlog4j.configuration=file:./custom-log4j.properties \
        -Dspark.ctl.url=$ctlUrl \
        -Dspark.ctl.loading.id=$loadingId \
        -Dspark.ctl.wf.id=$wfId \
        -Dspark.ctl.loading.start='$loadingStart' \
        -Dspark.ctl.api.version.v5='$ctlApiVersionV5' \
        -Dspark.property.path=$propertyPath \
        -Dspark.system.property.path=$systemPropertyPath \
        -Dspark.custom.build.date=$customBuildDate
" \
personalization.jar
