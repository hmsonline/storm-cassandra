//
// Copyright (c) 2013 Health Market Science, Inc.
//
package com.hmsonline.storm.cassandra.exceptions;

public class TupleMappingException extends RuntimeException {

    /**
     * This exception caused by bad data format in a tuple. A mapper should
     * throw this error so that storm would not try to replay the tuple
     */
    private static final long serialVersionUID = 5748318667106957380L;

    public TupleMappingException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
