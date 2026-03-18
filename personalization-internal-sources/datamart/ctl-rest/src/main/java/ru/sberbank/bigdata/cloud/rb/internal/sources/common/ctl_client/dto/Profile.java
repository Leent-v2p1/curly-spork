package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto;

import java.util.List;

public class Profile  {

    public final int id;
    public final String name;
    public final String description;
    public final String oozieUri;
    public final String hueUri;

    public Profile(int id, String name, String description, String oozieUri, String hueUri) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.oozieUri = oozieUri;
        this.hueUri = hueUri;
    }
}
