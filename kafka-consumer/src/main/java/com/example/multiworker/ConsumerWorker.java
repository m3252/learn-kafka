package com.example.multiworker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumerWorker implements Runnable{

    private final static Logger log = LoggerFactory.getLogger(ConsumerWorker.class);
    private String recordValue;

    public ConsumerWorker(String recordValue) {
        this.recordValue = recordValue;
    }

    @Override
    public void run() {
        log.info("thread:{} \trecord:{}", Thread.currentThread().getName(), recordValue);
    }
}
