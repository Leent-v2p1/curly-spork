
<#if executorMemory??>
                --executor-memory ${executorMemory}
                </#if>
                <#if executorCoreNum??>
                --executor-cores ${executorCoreNum}
                </#if>
                <#if numExecutors??>
                --num-executors ${numExecutors}
                </#if>
                <#if driverMemory??>
                --driver-memory ${driverMemory}
                </#if>
                --files ${r"${nameNode}${keytabPath}"}<#if log4j??>,${log4j}</#if>
                --queue ${r"${yarnQueue}"}
                <#if attempts??>
                --conf spark.yarn.maxAppAttempts=${attempts}
                </#if>
                --conf spark.driver.extraJavaOptions=
                    -Dhive.metastore.uris=${r"${hiveMetastoreUri}"}
                    -Dspark.graphite.carbon.host=${r"${graphite.carbon.host}"}
                    -Dspark.graphite.carbon.port=${r"${graphite.carbon.port}"}
                    -Dhive.metastore.sasl.enabled=true
                    -Dspark.hiveJdbcUrl=${r"${hiveJdbcUrl}"}
                    -Dspark.principal=${r"${principal}"}
                    -Dspark.user.name=${r"${wf:conf('user.name')}"}
                    -Dspark.keytabPath=${r"${replaceAll(keytabPath, '/keytab/', '')}"}
                    -Dspark.skipIfBuiltToday=${r"${wf:conf('skipIfBuiltToday')}"}
                    <#if log4j??>
                    -Dlog4j.configuration=custom-log4j.properties
                    </#if>
                    <#if executorMemoryOverhead??>
                    -Dspark.yarn.executor.memoryOverhead=${executorMemoryOverhead?c}
                    </#if>
                    <#if shufflePartitions??>
                    -Dspark.sql.shuffle.partitions=${shufflePartitions?c}
                    </#if>
                    <#if driverMemoryOverhead??>
                    -Dspark.yarn.driver.memoryOverhead=${driverMemoryOverhead?c}
                    </#if>
                    -Dspark.ctl.url=${r"${ctl}"}
                    -Dspark.ctl.loading.id=${r"${loading_id}"}
                    -Dspark.start.time='${r"${loading_start}"}'
                    <#if databaseStgName??>
                    -Dstg.database.name=${databaseStgName}
                    </#if>
                    <#if databaseName??>
                    -Dspark.pa.database.name=${databaseName}
                    </#if>
                    <#if resultSchema??>
                    -Dspark.result.schema=${resultSchema}
                    </#if>
                    <#if resultTable??>
                    -Dspark.result.table=${resultTable}
                    </#if>
                    -Dspark.wf.id=${r"${wf_id}"}
                    -Dspark.properties.path=${r"${propertiesPath}"}
                    -Dspark.environment=${r"${environment}"}
                    -Dspark.recovery.date='${r"${wf:conf('recoveryDate')}"}'
                    -Dspark.custom.build.date='${r"${wf:conf('customBuildDate')}"}'
                    <#if recoveryDate??>
                    -Dspark.recoveryDate=${recoveryDate}
                    </#if>
                    <#if datamartTableName??>
                    -Dspark.datamart.parent.table.name=${datamartTableName}
                    </#if>
                    <#if historyTableName??>
                    -Dspark.datamart.history.table.name=${historyTableName}
                    </#if>
