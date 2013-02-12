package com.hmsonline.storm.cassandra.exceptions;

/**
 * Exception indicating that a recoverable error has occurred when
 * interacting with Cassandra, and Storm <i>should</i>
 * replay a tuple.
 * 
 * @author tgoetz
 *
 */
public class StormCassandraException extends RuntimeException {

    private static final long serialVersionUID = 7670698606144390443L;

    public StormCassandraException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
