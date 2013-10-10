package com.hmsonline.storm.cassandra.exceptions;


import storm.trident.operation.TridentCollector;

import java.io.Serializable;

public interface ExceptionHandler extends Serializable {

    /**
     * Called by CassandraState when an exception is encountered.
     *
     * The TridentCollection parameter is provided for reporting
     * errors and optionally emitting tuples.
     *
     * Note that the <code>Collector</code> parameter may be null if
     * an exception occurs outside of a context where a TridentCollector
     * is available.
     *
     * @param ex
     * @param collector
     */
    void onException(Exception ex, TridentCollector collector);
}
