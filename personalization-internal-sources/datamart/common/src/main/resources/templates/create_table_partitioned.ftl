CREATE <#if extLocation??>EXTERNAL</#if> TABLE IF NOT EXISTS ${tableName}( ${columns} )
<#if partitionBy??>
PARTITIONED BY
(${partitionBy})
</#if>
STORED AS PARQUET
<#if extLocation??>LOCATION '${extLocation}'</#if>