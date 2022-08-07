package com.example;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class FilterProcessor implements Processor<String, String> {

    private ProcessorContext context;


    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(String key, String value) {
        if (value.length() > 5) {
            context.forward(key, value);
        }
    }

    @Override
    public void close() {
    }
}
