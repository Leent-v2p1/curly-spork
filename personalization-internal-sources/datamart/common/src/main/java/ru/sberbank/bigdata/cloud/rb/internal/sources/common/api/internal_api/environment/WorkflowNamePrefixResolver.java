package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment;

public class WorkflowNamePrefixResolver {

    public static final String PROD_NAME = "prod";
    public static final String TEST_NAME = "test";

    private final Environment env;

    public WorkflowNamePrefixResolver(Environment env) {
        this.env = env;
    }

    public String resolve() {
        switch (env) {
            case PRODUCTION:
                return PROD_NAME;
            case PRODUCTION_TEST:
                return TEST_NAME;
            default:
                throw new IllegalArgumentException("There is no environment with type" + env.name());
        }
    }
}
