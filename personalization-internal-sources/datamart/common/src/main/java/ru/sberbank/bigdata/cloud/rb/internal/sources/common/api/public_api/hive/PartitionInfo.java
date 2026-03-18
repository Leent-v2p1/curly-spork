package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.hive;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.Checker;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class PartitionInfo {
    private final List<Partition> partitions;
    private final boolean dynamic;

    private static final Logger log = LoggerFactory.getLogger(PartitionInfo.class);

    private PartitionInfo(boolean dynamic, List<Partition> partitions) {
        this.dynamic = dynamic;
        this.partitions = partitions;
    }

    public static PartitionInfoDynamicBuilder dynamic() {
        return new PartitionInfoDynamicBuilder();
    }

    public static PartitionInfoNonDynamicBuilder nonDynamic() {
        return new PartitionInfoNonDynamicBuilder();
    }

    public List<Partition> partitions() {
        return partitions;
    }

    public Optional<Partition> getPartition(String colName) {
        if (isDynamic()) {
            for (Partition part : partitions) {
                if (part.name.equalsIgnoreCase(colName)) {
                    return Optional.of(part);
                }
            }
            return Optional.empty();
        } else {
            throw new IllegalStateException("non dynamic partition cannot be found only by name");
        }
    }

    public List<String> colNames() {
        return partitions.stream().map(partition -> partition.name).collect(toList());
    }

    public Column[] columns() {
        return partitions.stream().map(partition -> col(partition.name)).toArray(Column[]::new);
    }

    public String toPath() {
        if (isDynamic()) {
            throw new IllegalStateException("dynamic partition cannot be mapped to path");
        } else {
            return partitions().stream().map(p -> p.name + "=" + p.value).collect(joining("/"));
        }
    }

    public String whereCondition() {
        if (isDynamic()) {
            throw new IllegalStateException("dynamic partition cannot be mapped to where condition");
        } else {
            return partitions().stream().map(p -> p.name + " = '" + p.value + "'").collect(joining(" and "));
        }
    }

    public boolean isDynamic() {
        return dynamic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionInfo that = (PartitionInfo) o;
        return dynamic == that.dynamic &&
                Objects.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitions, dynamic);
    }

    @Override
    public String toString() {
        return "PartitionInfo{" +
                "partitions=" + partitions +
                ", dynamic=" + dynamic +
                '}';
    }

    public static class Partition {
        public final String name;
        public final DataType type;
        public final String value;

        public Partition(String name, DataType type) {
            this.name = name;
            this.type = type;
            this.value = null;
        }

        public Partition(String name, DataType type, String value) {
            this.name = name;
            this.type = type;
            this.value = value;
        }

        @Override
        public String toString() {
            return "Partition{" +
                    "name='" + name + '\'' +
                    ", value='" + value + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Partition partition = (Partition) o;
            return Objects.equals(name, partition.name) &&
                    Objects.equals(value, partition.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, value);
        }
    }


    public static class PartitionInfoDynamicBuilder {
        private final List<Partition> partitions = new ArrayList<>();

        public PartitionInfoDynamicBuilder add(String name) {
            partitions.add(new Partition(name, StringType));
            return this;
        }

        public PartitionInfoDynamicBuilder add(String name, DataType type) {
            partitions.add(new Partition(name, type));
            return this;
        }

        public PartitionInfo create() {
            log.warn("Partitions in info {}", partitions);
            Checker.checkCondition(partitions.isEmpty(), "Partition must have at least 1 column");
            return new PartitionInfo(true, partitions);
        }
    }


    public static class PartitionInfoNonDynamicBuilder {
        private final List<Partition> partitions = new ArrayList<>();

        public PartitionInfoNonDynamicBuilder add(String name, String value) {
            partitions.add(new Partition(name, StringType, value));
            return this;
        }

        public PartitionInfoNonDynamicBuilder add(String name, DataType type, String value) {
            partitions.add(new Partition(name, type, value));
            return this;
        }

        public PartitionInfoNonDynamicBuilder add(List<Partition> partitions) {
            this.partitions.addAll(partitions);
            return this;
        }

        public PartitionInfo create() {
            Checker.checkCondition(partitions.isEmpty(), "Partition must have at least 1 column");
            return new PartitionInfo(false, partitions);
        }
    }
}


