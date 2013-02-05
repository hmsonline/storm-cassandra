package com.hmsonline.storm.cassandra.bolt;

import java.io.Serializable;

import com.netflix.astyanax.annotations.Component;

public class SimpleComposite implements Serializable{
    @Component(ordinal = 0)
    public String part1;
    
    @Component(ordinal = 1)
    public String part2;
    
    public SimpleComposite(){
        
    }
    
    public SimpleComposite(String part1, String part2){
        this.part1 = part1;
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
