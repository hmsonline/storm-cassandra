//
// Copyright (c) 2013 Health Market Science, Inc.
//
package com.hmsonline.storm.cassandra.exceptions;

/**
 * Exception indicating that a <code>TupleMapper</code> 
 * implementation has failed to map a storm <code>Tuple</code>
 * properly, and that repeated mapping attempts would
 * never succeed for a given tuple.
 * 
 * <code>TupleMapper</code> implementations should throw
 * this error when some sort of data format error has
 * occurred, indicating that storm should NOT replay
 * the tuple.
 * 
 * 
 * @author tgoetz
 *
 */
public class TupleMappingException extends RuntimeException {

    private static final long serialVersionUID = 5748318667106957380L;

    public TupleMappingException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
