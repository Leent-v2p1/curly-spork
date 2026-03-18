<workflow-app name="create-datamart-${tableName}"<#noparse> xmlns="uri:oozie:workflow:0.4">
    <global>
        <configuration>
            <property>
                <name>mapred.job.queue.name</name>
                <value>${yarnQueue}</value>
            </property>
        </configuration>
    </global>
</#noparse>
    <start to="${firstWf}"/>

<#list wfs as wf>
    <action name="${wf.name}">
        <sub-workflow>
            <app-path>${wf.appPath}</app-path>
            <propagate-configuration/>
            <configuration>
                <property>
                    <name>customBuildDate</name>
                    <value>${wf.customBuildDate}</value>
                </property>
            </configuration>
        </sub-workflow>
        <ok to="${wf.nextName}"/>
        <error to="kill"/>
    </action>
</#list>
<#noparse>
    <kill name="kill">
        <message>Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
</#noparse>