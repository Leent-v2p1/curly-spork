# Generated parameters
executorMemory=${executorMemory}
executorCoreNum=${executorCoreNum}
executors=${numExecutors}
driverMemory=${driverMemory}
# Parameters from ctl
principal=$[principal]
keytabPath=$[keytabPath]
queue=$[yarnQueue]
metastore_uri=$[hiveMetastoreUri]
user_name=$[user.name]
# Additional ctl execution parameters
[ctlExecutionConf]