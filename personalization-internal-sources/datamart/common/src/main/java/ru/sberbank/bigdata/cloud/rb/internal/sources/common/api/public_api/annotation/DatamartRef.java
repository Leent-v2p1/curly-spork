package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DatamartRef {
    String id();

    String name();

    String step() default "";

    String stepName() default "";

    /**
     * if true AutoConfigDatamartRunner resolves datamart name from system properties: result.table and result.schema
     */
    boolean useSystemPropertyToGetId() default false;
}
