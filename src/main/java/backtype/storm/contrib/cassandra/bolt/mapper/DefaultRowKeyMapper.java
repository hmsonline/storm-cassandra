package backtype.storm.contrib.cassandra.bolt.mapper;

import backtype.storm.tuple.Tuple;

public class DefaultRowKeyMapper implements RowKeyMapper {
    private String keyField;

    public DefaultRowKeyMapper(String keyField) {
        this.keyField = keyField;
    }

    @Override
    public Object mapRowKey(Tuple tuple) {
        return tuple.getValueByField(this.keyField);
    }

}
