package backtype.storm.contrib.cassandra.bolt.mapper;

import backtype.storm.tuple.Tuple;

public class DefaultColumnFamilyMapper implements ColumnFamilyMapper {

    private String columnFamily;

    public DefaultColumnFamilyMapper(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    @Override
    public String mapToColumnFamily(Tuple tuple) {
        return this.columnFamily;
    }

}
