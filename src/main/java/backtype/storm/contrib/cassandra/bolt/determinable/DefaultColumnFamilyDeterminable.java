package backtype.storm.contrib.cassandra.bolt.determinable;

import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class DefaultColumnFamilyDeterminable implements
		ColumnFamilyDeterminable {
	
	private String columnFamily;
	
	public DefaultColumnFamilyDeterminable(String columnFamily){
		this.columnFamily = columnFamily;
	}

	@Override
	public String determineColumnFamily(Tuple tuple) {
		return this.columnFamily;
	}

}
