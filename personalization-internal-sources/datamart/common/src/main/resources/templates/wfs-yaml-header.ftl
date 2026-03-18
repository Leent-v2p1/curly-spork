---
global:
    hive:
        principal: '{{common.hive.principal}}'
        metastore-uri: '{{common.hive.metastore.uri}}'
        jdbc-url: '{{common.hive.jdbc.uri}}'

    graphite:
        carbon:
            host: '{{common.metric.graphite.carbon.host}}'
            port: '{{common.metric.graphite.carbon.port}}'
    user:
        name: '${username}'
    profile: '{{common.ctl_v5.profile}}'
    wf_prefix: '{{common.ctl_v5.workflow.prefix}}'
    keytabPath: '${keytabPath}'
    parent-directory: /data/custom/rb/
    yarn-queue: '${queue}'
    environment: ${environment}
    properties-path: ${propertiesPath}
    ctl-version-required: ${ctlVersionRequired}
    ctl-version-api-v5: 'true'

workflows: