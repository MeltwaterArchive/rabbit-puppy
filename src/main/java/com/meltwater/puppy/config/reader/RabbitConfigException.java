package com.meltwater.puppy.config.reader;

public class RabbitConfigException extends Exception {
    public RabbitConfigException(String s, Exception e) {
        super(s, e);
    }

    public RabbitConfigException(String error) {
        super(error);
    }
}
