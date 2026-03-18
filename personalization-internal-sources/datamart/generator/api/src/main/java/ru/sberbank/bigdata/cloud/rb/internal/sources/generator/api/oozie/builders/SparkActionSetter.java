package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.BaseProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.components.properties_generators.GeneratedSparkOptions;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.spark_action.ACTION;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;

public class SparkActionSetter {
    private final Environment env;
    private final GeneratedSparkOptions sparkOptions;
    private final BaseProperties properties = new BaseProperties();

    public SparkActionSetter(Environment env, GeneratedSparkOptions sparkOptions) {
        this.env = env;
        this.sparkOptions = sparkOptions;
    }

    public void setAction(ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.ACTION action,
                          ActionConf actionConf) {
        action.setCred("howlauth");

        ACTION spark = new ACTION();
        spark.setJobTracker("${jobTracker}");
        spark.setNameNode("${nameNode}");
        spark.setMaster("yarn-cluster");
        spark.setMode("cluster");
        spark.setName(actionConf.getName());
        spark.setClazz(actionConf.getClassName());
        spark.setJar(properties.getJarPathForEnv(env));
        spark.setSparkOpts(sparkOptions.createOptionsAccordingToTemplate());

        action.setSpark(spark);
    }
}
