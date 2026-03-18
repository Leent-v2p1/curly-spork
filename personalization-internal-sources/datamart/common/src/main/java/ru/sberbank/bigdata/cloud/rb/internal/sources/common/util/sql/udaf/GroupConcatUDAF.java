package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.udaf;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createArrayType;

/**
 * User defined aggregate function for concatenating strings,
 * analog of mysql GROUP_CONCAT function
 */
public class GroupConcatUDAF extends UserDefinedAggregateFunction {

    @Override
    public StructType inputSchema() {
        return new StructType().add("string", StringType);
    }

    @Override
    public StructType bufferSchema() {
        return new StructType().add("buff", createArrayType(StringType));
    }

    @Override
    public DataType dataType() {
        return StringType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, new ArrayList<String>());
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        if (!input.isNullAt(0)) {
            List<String> update = new ArrayList<>(buffer.getList(0));
            update.add(input.getString(0));
            buffer.update(0, update);
        }
    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        List<String> seq1 = buffer1.getList(0);
        List<String> seq2 = buffer2.getList(0);

        List<String> merge = new ArrayList<>(seq1);
        merge.addAll(seq2);

        buffer1.update(0, merge);
    }

    @Override
    public Object evaluate(Row buffer) {
        return String.join("", buffer.<String>getList(0));
    }
}
