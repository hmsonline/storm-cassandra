package com.hmsonline.storm.cassandra.bolt.mapper;

import java.io.Serializable;
import java.util.List;

import storm.trident.tuple.TridentTuple;

import backtype.storm.tuple.Values;

public interface TridentColumnMapper<T>  extends Serializable {

    /**
     * Given a set of columns, maps to values to emit.
     * @param columns
     * @return
     */
    public List<Values> mapToValues(String rowKey, Columns<T> columns, TridentTuple input);
}
