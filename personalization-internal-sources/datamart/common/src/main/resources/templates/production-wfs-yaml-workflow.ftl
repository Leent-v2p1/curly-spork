
    - ${ctlWorkflowName}-wf:
        name: ${ctlWorkflowName}
        category: ${category}
        type: principal
        profile: {{common.ctl_v5.profile}}
        singleLoading: true
        orchestrator: oozie
        schedule_params:
        <#if (dependencyConf.eventAwaitStrategy)??>
            eventAwaitStrategy: "${dependencyConf.eventAwaitStrategy}"
        </#if>
        <#if (dependencyConf.cron)??>
            cron:
                expression: "${dependencyConf.cron.expression}"
                active: ${dependencyConf.cron.active?c}
        </#if>
        <#if dependencyConf.replicaDependency?? || dependencyConf.datamartDependency??>
            entities:
        </#if>
        <#if (dependencyConf.replicaDependency)??>
            <#list dependencyConf.replicaDependency as replicaDependency>
                <#list replicaDependency.entities as entity>
                - entity:
                    id: ${entity?c}
                    profile: <#if (replicaDependency.profile)?has_content>${replicaDependency.profile}<#else>{{common.ctl_v5.source_stat_profile}}</#if>
                    statisticId: ${replicaDependency.statId?c}
                    active: true
                </#list>
            </#list>
        </#if>
        <#if (dependencyConf.datamartDependency)??>
            <#list dependencyConf.datamartDependency as datamartDependency>
                <#list datamartDependency.entities as entity>
                - entity:
                    id: ${entity?c}
                    profile: {{common.ctl_v5.profile}}
                    statisticId: ${datamartDependency.statId?c}
                    active: true
                </#list>
            </#list>
        </#if>
        <#if dependencyConf.locks?? && ( dependencyConf.locks.checks?? || dependencyConf.locks.sets?? )>
        init_locks:
            <#if (dependencyConf.locks.checks)??>
            checks:
                <#list dependencyConf.locks.checks as check>
                - check:
                    entity_id: ${check.entityId}
                    lock: ${check.type}
                    lock_group: ${check.lockGroup}
                    <#if (check.profile)??>
                    profile: ${check.profile}
                    <#else>
                    profile: {{common.ctl_v5.profile}}
                    </#if>
                </#list>
            </#if>
            <#if (dependencyConf.locks.sets)??>
            sets:
                <#list dependencyConf.locks.sets as set>
                - set:
                    entity_id: ${set.entityId}
                    lock: ${set.type}
                    lock_group: ${set.lockGroup}
                    estimate: ${set.estimate}
                    <#if (set.profile)??>
                    profile: ${set.profile}
                    <#else>
                    profile: {{common.ctl_v5.profile}}
                    </#if>
                </#list>
            </#if>
        </#if>
        params:
            - distribution-build-time:
                name: distribution.build.time
                prior_value: ${distributionBuildTime}
            - distribution-build-version:
                name: distribution.build.version
                prior_value: '{{common.build.version}}'
            - application-name:
                name: appName
                prior_value: ${ctlWorkflowName}
            - application-path:
                name: oozie.wf.application.path
                prior_value: ${appPath}
            - user-name:
                name: user.name
            <#if dependencyConf.parameters.getUserName()??>
                <#if dependencyConf.parameters.getUserName() = "pnl">
                prior_value: {{username_aggr}}
                <#elseif dependencyConf.parameters.getUserName() = "evo">
                prior_value: {{username_evo}}
                <#else>
                prior_value: {{username_default}}_${dependencyConf.parameters.getUserName()}
                </#if>
            <#else>
                prior_value: <#noparse>${global.user.name}</#noparse>
            </#if>
            - hive-user:
                name: principal
                prior_value: <#noparse>${global.hive.principal}</#noparse>
            - metastore-uri:
                name: hiveMetastoreUri
                prior_value: <#noparse>${global.hive.metastore-uri}</#noparse>
            - jdbc-url:
                name: hiveJdbcUrl
                prior_value: <#noparse>${global.hive.jdbc-url}</#noparse>
            - graphite-carbon-host:
                name: graphite.carbon.host
                prior_value: <#noparse>${global.graphite.carbon.host}</#noparse>
            - graphite-carbon-port:
                name: graphite.carbon.port
                prior_value: <#noparse>${global.graphite.carbon.port}</#noparse>
            - table-directory:
                name: tableParentDir
                prior_value: <#noparse>${global.parent-directory}</#noparse>
            - yarn-queue:
                name: yarnQueue
                prior_value: <#noparse>${global.yarn-queue}</#noparse>
            - properties-path:
                name: propertiesPath
                prior_value: <#noparse>${global.properties-path}</#noparse>
            - environment:
                name: environment
                prior_value: <#noparse>${global.environment}</#noparse>
            - ctl-version-required:
                name: ctlVersionRequired
                prior_value: <#noparse>${global.ctl-version-required}</#noparse>
            - ctl-version-api-v5:
                name: ctlApiVersionV5
                prior_value: <#noparse>${global.ctl-version-api-v5}</#noparse>
            - oozie-use-system-libpath:
                name: oozie.use.system.libpath
                prior_value: true
            - keytabPath:
                name: keytabPath
                <#if dependencyConf.parameters.getUserName()??>
                    <#if dependencyConf.parameters.getUserName() = "pnl">
                prior_value: /keytab/{{username_aggr}}.keytab
                    <#elseif dependencyConf.parameters.getUserName() = "evo">
                prior_value: /keytab/{{username_evo}}.keytab
                    <#else>
                prior_value: /keytab/{{username_default}}_${dependencyConf.parameters.getUserName()}.keytab
                    </#if>
                <#else>
                prior_value: <#noparse>${global.keytabPath}</#noparse>
                </#if>
            - skip-if-built-today:
                name: skipIfBuiltToday
                prior_value: ${dependencyConf.parameters.getSkipIfBuiltToday()?c}
            <#if sourceInstance??>
            - source-instance:
                name: ${sourceInstance.ctlParamName}
                prior_value: ${sourceInstance.value}
            </#if>
            <#if dependencyConf.parameters.getStartDt()??>
            - start-dt:
                name: start_dt
                prior_value: ${dependencyConf.parameters.getStartDt()}
            </#if>
        schedule:
            type: <#if dependencyConf??>${scheduleType}<#else>none</#if>
        notifications:
          - { status: "ERROR", emails: [ "CAB-GD-ADDR-005737@omega.sbrf.ru" ] }