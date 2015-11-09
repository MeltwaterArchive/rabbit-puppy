package com.meltwater.puppy.config;

import java.util.HashMap;
import java.util.Map;

public class QueueData {
    private boolean durable = true;
    private boolean auto_delete = false;
    private Map<String, Object> arguments = new HashMap<>();

    public QueueData() {}

    public QueueData(boolean durable, boolean auto_delete, Map<String, Object> arguments) {
        this.durable = durable;
        this.auto_delete = auto_delete;
        setArguments(arguments);
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public boolean isAuto_delete() {
        return auto_delete;
    }

    public void setAuto_delete(boolean auto_delete) {
        this.auto_delete = auto_delete;
    }

    public Map<String, Object> getArguments() {
        return arguments;
    }

    public void setArguments(Map<String, Object> arguments) {
        if (arguments == null) {
            this.arguments = new HashMap<>();
        } else {
            this.arguments = arguments;
        }
    }

    public QueueData addArgument(String key, Object value) {
        arguments.put(key, value);
        return this;
    }

    @Override
    public String toString() {
        return "QueueData{" +
                "durable=" + durable +
                ", auto_delete=" + auto_delete +
                ", arguments=" + arguments +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueueData queueData = (QueueData) o;

        if (durable != queueData.durable) return false;
        if (auto_delete != queueData.auto_delete) return false;
        return !(arguments != null ? !arguments.equals(queueData.arguments) : queueData.arguments != null);

    }

    @Override
    public int hashCode() {
        int result = (durable ? 1 : 0);
        result = 31 * result + (auto_delete ? 1 : 0);
        result = 31 * result + (arguments != null ? arguments.hashCode() : 0);
        return result;
    }
}
