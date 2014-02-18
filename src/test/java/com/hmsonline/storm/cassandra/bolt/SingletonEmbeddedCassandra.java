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

import com.netflix.astyanax.test.EmbeddedCassandra;

public class SingletonEmbeddedCassandra {
    
    private static class Holder {
        private static final SingletonEmbeddedCassandra instance = new SingletonEmbeddedCassandra();
    }

    private final EmbeddedCassandra         cassandra;

    private SingletonEmbeddedCassandra() {
        try {
            cassandra = new EmbeddedCassandra("target/cassandra-data");
            cassandra.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start embedded cassandra", e);
        }
    }
    
    public static SingletonEmbeddedCassandra getInstance() {
        return Holder.instance;
    }
    
    public void finalize() {
        try {
            cassandra.stop();
        }
        catch (Exception e) {
            
        }
    }
}
