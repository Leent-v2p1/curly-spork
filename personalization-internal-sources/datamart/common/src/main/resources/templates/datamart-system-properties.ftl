# Spark default parameters
[sparkAppDefaultConf]
spark.sql.autoBroadcastJoinThreshold=524288000
# Generated parameters
<#if executorMemoryOverhead??>
spark.yarn.executor.memoryOverhead=${executorMemoryOverhead?c}
</#if>
<#if shufflePartitions??>
spark.sql.shuffle.partitions=${shufflePartitions?c}
</#if>
<#if attempts??>
spark.yarn.maxAppAttempts=${attempts}
</#if>
<#if databaseStgName??>
spark.stg.database.name=${databaseStgName}
</#if>
<#if databaseName??>
spark.pa.database.name=${databaseName}
</#if>
<#if className??>
spark.class.name=${className}
</#if>
<#if datamartClassName??>
spark.datamart.class.name=${datamartClassName}
</#if>
<#if monthlyDependencyDay??>
spark.monthly.dependency.day=${monthlyDependencyDay}
</#if>
<#if sourceSchema??>
spark.source.schema=${sourceSchema}
</#if>
<#if resultSchema??>
spark.result.schema=${resultSchema}
</#if>
<#if resultTable??>
spark.result.table=${resultTable}
</#if>
<#if ctlEntityId??>
spark.ctl.entity.id=${ctlEntityId?c}
</#if>
<#if workflowType??>
spark.workflow.type=${workflowType}
</#if>
<#if recoveryDate??>
spark.recoveryDate=${recoveryDate}
</#if>
<#if datamartTableName??>
spark.datamart.parent.table.name=${datamartTableName}
</#if>
<#if historyTableName??>
spark.datamart.history.table.name=${historyTableName}
</#if>
<#if emergencyStop??>
spark.emergencyStop=${emergencyStop}
</#if>
<#if customJavaOpts??>
<#list customJavaOpts as customJavaOpt>
${customJavaOpt.key}=<#if customJavaOpt.value?is_boolean>${customJavaOpt.value?c}<#else>${customJavaOpt.value}</#if>
</#list>
</#if>
# Parameters from ctl
spark.graphite.carbon.host=
spark.graphite.carbon.port=$[graphite.carbon.port]
spark.skipIfBuiltToday=$[skipIfBuiltToday]
spark.hiveJdbcUrl=$[hiveJdbcUrl]
spark.principal=$[principal]
spark.user.name=$[user.name]
spark.keytabPath=$[keytabPath]
spark.ctl.url=$[ctl.url]
spark.ctl.loading.id=$[ctl.loading.id]
spark.wf.id=$[ctl.wf.id]
spark.start.time=$[ctl.loading.start]
spark.properties.path=$[propertiesPath]
spark.ctl.api.version.v5=$[ctlApiVersionV5]
spark.environment=$[environment]
spark.recovery.date=$[recoveryDate]
spark.custom.build.date=$[custom.build.date]
spark.start_dt=$[start_dt]
# Custom parameters from ctl
[ctlCustomConf]