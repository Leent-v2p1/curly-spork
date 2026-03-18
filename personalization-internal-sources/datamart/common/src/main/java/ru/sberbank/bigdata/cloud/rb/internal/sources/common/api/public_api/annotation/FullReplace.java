package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation used for simple non-historical non-partitioned datamarts
 * that will fully overwrites in every load
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface FullReplace {
}
