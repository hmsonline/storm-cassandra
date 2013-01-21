package com.hmsonline.storm.cassandra.exceptions;

//
// Copyright (c) 2013 Health Market Science, Inc.
//

public class StormCassandraException extends RuntimeException {

    /**
     * This exception is thrown when unexpected errors occur that prevent
     * interaction with Cassandra. Storm will replay tuple when catch this
     * exception
     */
    private static final long serialVersionUID = 7670698606144390443L;

    public StormCassandraException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
