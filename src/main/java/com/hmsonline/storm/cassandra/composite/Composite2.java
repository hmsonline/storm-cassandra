package com.hmsonline.storm.cassandra.composite;

import com.netflix.astyanax.annotations.Component;

public class Composite2<A, B> {

    @Component(ordinal = 0)
    public A a;
    @Component(ordinal = 1)
    public B b;
    
    public Composite2(){
        
    }
    
    public Composite2(A part1, B part2){
        this.a = part1;
        this.b = part2;
    }

    public A getA() {
        return a;
    }

    public void setA(A a) {
        this.a = a;
    }

    public B getB() {
        return b;
    }

    public void setB(B b) {
        this.b = b;
    }
}
