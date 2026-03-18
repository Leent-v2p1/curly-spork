package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum;

public class GreenplumConstants {
    public static final String GP_URL = "jdbc:postgresql://gp_dns_pkap1030_daas_rozn2.gp.df.sbrf.ru:5432/gp_rozn2?jaasApplicationName=GpClient";
    public static final String GP_USER = "u_sklgrnplm_s_vd_rozn_mpp_daas_tech";
    public static final String GP_PARTITION_COLUMN= "gp_seg_id";
    public static final int GP_PARTITIONS = 100;
}
