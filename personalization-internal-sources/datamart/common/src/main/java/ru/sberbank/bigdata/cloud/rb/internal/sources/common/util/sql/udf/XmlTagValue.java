package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.udf;

import org.apache.spark.sql.api.java.UDF2;

/**
 * returns input tag value from xml
 */
public class XmlTagValue implements UDF2<String, String, String> {

    @Override
    public String call(String xml, String tag) {
        if (xml == null) {
            return null;
        }

        String[] withTag = xml.split("<" + tag + ">");
        return (withTag.length == 1) ? null : withTag[1].split("</" + tag + ">")[0];
    }
}
