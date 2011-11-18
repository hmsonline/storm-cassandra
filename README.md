Storm Cassandra Integration
===========================

Integrates Storm and Cassandra by providing a generic and configurable `Bolt` implementation that writes Storm `Tuple` objects to a Cassandra Column Family.

How the Storm `Tuple` data is written to Cassandra is dynamically configurable -- you provide provide classes that "determine" a column family, row key, and column name/values, and the bolt will write the data to a Cassandra cluster.

