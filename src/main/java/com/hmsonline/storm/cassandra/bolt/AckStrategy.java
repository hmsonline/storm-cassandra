package com.hmsonline.storm.cassandra.bolt;

/**
 * Represents an "acknowldedgement strategy" for Cassandra bolts.
 * 
 * ACK_IGNORE: The Cassandra bolt will ignore (i.e. not acknowledge)
 * tuples received, regardless of whether the Cassandra write succeeded
 * or failed.
 * 
 * ACK_ON_RECIEVE: The Cassandra bolt will ack all tuples when received
 * by the bolt, regardless of whether the Cassandra write succeeded
 * or failed.
 * 
 * ACK_ON_WRITE: The Cassandra bolt will only ack tuples after the
 * resulting cassandra write(s) (potentially batched), have all
 * succeeded.
 * 
 * @author tgoetz
 *
 */
public enum AckStrategy {
    ACK_IGNORE, 
    ACK_ON_RECEIVE, 
    ACK_ON_WRITE;
}
