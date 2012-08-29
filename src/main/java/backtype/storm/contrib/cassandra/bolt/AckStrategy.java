package backtype.storm.contrib.cassandra.bolt;

public enum AckStrategy {
    ACK_IGNORE, ACK_ON_RECEIVE, ACK_ON_WRITE;
}
