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
package com.hmsonline.storm.cassandra.exceptions;

import backtype.storm.topology.FailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;

import java.util.HashSet;

public class MappedExceptionHandler implements ExceptionHandler {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MappedExceptionHandler.class);


    public static enum Action{
        FAIL_TUPLE, REPORT_ERROR, KILL_WORKER, LOG
    }

    private HashSet<Class<? extends Exception>> failTupleExceptions = new HashSet<Class<? extends Exception>>();
    private HashSet<Class<? extends Exception>> reportErrorExceptions = new HashSet<Class<? extends Exception>>();
    private HashSet<Class<? extends Exception>> killWorkerExceptions = new HashSet<Class<? extends Exception>>();

    private Action defaultAction = Action.LOG;

    protected final void setDefaultAction(Action action){
        this.defaultAction = action;
    }

    protected final void addFailOn(Class<? extends Exception>... exceptions) {
        for (Class<? extends Exception> e : exceptions) {
            if (reportErrorExceptions.contains(e) || killWorkerExceptions.contains(e)) {
                throw new IllegalStateException("Exception " + e.getName() + " has already been registered.");
            }
            this.failTupleExceptions.add(e);
        }
    }

    protected final void addReportOn(Class<? extends Exception>... exceptions) {
        for (Class<? extends Exception> e : exceptions) {
            if (failTupleExceptions.contains(e) || killWorkerExceptions.contains(e)) {
                throw new IllegalStateException("Exception " + e.getName() + " has already been registered.");
            }
            this.reportErrorExceptions.add(e);
        }
    }

    protected final void addDieOn(Class<? extends Exception>... exceptions) {
        for (Class<? extends Exception> e : exceptions) {
            if (failTupleExceptions.contains(e) || reportErrorExceptions.contains(e)) {
                throw new IllegalStateException("Exception " + e.getName() + " has already been registered.");
            }
            this.killWorkerExceptions.add(e);
        }
    }

    protected final Action actionFor(Exception e){
        Class<? extends Exception> c = (Class<? extends Exception>)e.getClass();
        if(this.failTupleExceptions.contains(c)){
            return Action.FAIL_TUPLE;
        } else if(this.killWorkerExceptions.contains(c)){
            return Action.KILL_WORKER;
        } else if(this.reportErrorExceptions.contains(c)){
            return Action.REPORT_ERROR;
        } else{
            return this.defaultAction;
        }
    }


    public MappedExceptionHandler failOn(Class<? extends Exception>... exceptions) {
        addFailOn(exceptions);
        return this;
    }

    public MappedExceptionHandler dieOn(Class<? extends Exception>... exceptions) {
        addDieOn(exceptions);
        return this;
    }

    public MappedExceptionHandler reportOn(Class<? extends Exception>... exceptions) {
        addReportOn(exceptions);
        return this;
    }

    public MappedExceptionHandler withDefaultAction(Action action) {
        setDefaultAction(action);
        return this;
    }

    @Override
    public void onException(Exception e, TridentCollector collector) {
        switch (actionFor(e)) {
            case FAIL_TUPLE:
                LOG.info("Failing batch based on registered exception " + e.getClass().getName() + ", Message: '"
                        + e.getMessage() + "'", e);
                throw new FailedException(e);
            case REPORT_ERROR:
                LOG.info(
                        "Reporting error based on registered exception " + e.getClass().getName() + ", Message: '"
                                + e.getMessage() + "'", e);
                if(collector != null){
                    collector.reportError(e);
                }
                break;
            case KILL_WORKER:
                LOG.warn("Killing worker/executor based on registered exception " + e.getClass().getName()
                        + ", Message: '" + e.getMessage() + "'", e);
                // wrap in RuntimeException in case FailedException has been
                // explicitly registered.
                throw new RuntimeException(e);
            default:
                LOG.warn("Inoring exception " + e.getClass().getName() + " and acknowledging batch. Message: '"
                        + e.getMessage() + "'", e);
                break;
        }
    }

}
