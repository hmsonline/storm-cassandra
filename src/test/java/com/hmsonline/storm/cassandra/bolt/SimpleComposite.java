package com.hmsonline.storm.cassandra.bolt;

import com.netflix.astyanax.annotations.Component;

public class SimpleComposite {
    @Component(ordinal = 0)
    String part1;
    
    @Component(ordinal = 1)
    String part2;
    
    public SimpleComposite(String part1, String part2){
        this.part1 = part1;
        this.part2 = part2;
    }
}
