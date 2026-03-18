package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;

import java.util.List;
import java.util.Map;

public class BaseProperties {
    Map<String, Object> customProperties;

    public BaseProperties() {
        this(null);
    }

    BaseProperties(Map<String, Object> customProperties) {
        this.customProperties = customProperties;
    }

    protected Map<String, Object> properties() {
        if (customProperties != null) {
            return customProperties;
        }
        return Properties.get();
    }

    public String getJarPathForEnv(Environment env) {
        return (String) properties().get(env.nameLowerCase() + ".jarPath");
    }

    public String getGpPathForEnv(Environment env) {
        return (String) properties().get(env.nameLowerCase() + ".GpPath");
    }

    public String getJaasPathForEnv(Environment env) {
        return (String) properties().get(env.nameLowerCase() + ".JaasPath");
    }

    public String getPgPathForEnv(Environment env) {
        return (String) properties().get(env.nameLowerCase() + ".PgPath");
    }

    public String getKeystorePathForEnv(Environment env) {
        return (String) properties().get(env.nameLowerCase() + ".keystorePath");
    }

    public String getLogPathForEnv(Environment env) {
        return (String) properties().get(env.nameLowerCase() + ".logPath");
    }

    public String getPropertiesShellPathForEnv(Environment env) {
        return (String) properties().get(env.nameLowerCase() + ".propertiesShellPath");
    }

    public String getPropertiesPath(Environment env) {
        return (String) properties().get(env.nameLowerCase() + ".propertiesPath");
    }

    public String getCtlVersion(Environment env) {
        return (String) properties().get(env.nameLowerCase() + ".ctlVersionRequired");
    }

    public String getCtlApiVersionV5(Environment env){
        return String.valueOf(properties().get(env.nameLowerCase() + ".ctlApiVersionV5"));
    }

    public List<String> getOnSchedule(Environment env) {
        return (List<String>) properties().get(env.nameLowerCase() + ".onSchedule");
    }
}
