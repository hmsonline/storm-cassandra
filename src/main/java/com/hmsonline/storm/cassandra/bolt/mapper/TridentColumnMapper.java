package com.hmsonline.storm.cassandra.bolt.mapper;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public interface TridentColumnMapper<K, C, V> extends Serializable {

    /**
     * Given a set of columns, maps to values to emit.
     *
     * @param columns
     * @return
     */
    public List<Values> mapToValues(K rowKey, Map<C, V> columns, TridentTuple input);

    @Deprecated
    public List<Values> mapToValues(K rowKey, List<Map<C, V>> columns, TridentTuple input);
}
