/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
 *
 */
public enum AckStrategy {
    ACK_IGNORE, 
    ACK_ON_RECEIVE, 
    ACK_ON_WRITE;
}
