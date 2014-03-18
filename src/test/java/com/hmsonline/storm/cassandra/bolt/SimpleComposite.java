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

import java.io.Serializable;

import com.netflix.astyanax.annotations.Component;

@SuppressWarnings("serial")
public class SimpleComposite implements Serializable {

    @Component(ordinal = 0)
    private String part1;

    @Component(ordinal = 1)
    private String part2;

    public SimpleComposite() {
    }

    public SimpleComposite(String part1, String part2) {
        this.part1 = part1;
        this.part2 = part2;
    }

    public String getPart1() {
        return part1;
    }

    public void setPart1(String part1) {
        this.part1 = part1;
    }

    public String getPart2() {
        return part2;
    }

    public void setPart2(String part2) {
        this.part2 = part2;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((part1 == null) ? 0 : part1.hashCode());
        result = prime * result + ((part2 == null) ? 0 : part2.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof SimpleComposite)) {
            return false;
        }
        SimpleComposite other = (SimpleComposite) obj;
        if (part1 == null) {
            if (other.part1 != null) {
                return false;
            }
        } else if (!part1.equals(other.part1)) {
            return false;
        }
        if (part2 == null) {
            if (other.part2 != null) {
                return false;
            }
        } else if (!part2.equals(other.part2)) {
            return false;
        }
        return true;
    }

}
