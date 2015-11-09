package com.meltwater.puppy;

import java.util.List;

/**
 * Composite exception containing all exceptions found during
 */
public class RabbitPuppyException extends Exception {
    private final List<Throwable> errors;

    public RabbitPuppyException(String s, List<Throwable> errors) {
        super(s);
        this.errors = errors;
    }

    public List<Throwable> getErrors() {
        return errors;
    }
}
