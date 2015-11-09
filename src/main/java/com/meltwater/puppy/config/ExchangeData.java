package com.meltwater.puppy.config;


import java.util.HashMap;
import java.util.Map;

public class ExchangeData {

    private String type;
    private boolean durable = true;
    private boolean auto_delete = false;
    private boolean internal = false;
    private Map<String, Object> arguments = new HashMap<>();

    public ExchangeData() {}

    public ExchangeData(String type, boolean durable, boolean auto_delete, boolean internal, Map<String, Object> arguments) {
        this.type = type;
        this.durable = durable;
        this.auto_delete = auto_delete;
        this.internal = internal;
        setArguments(arguments);
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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

    public boolean isInternal() {
        return internal;
    }

    public void setInternal(boolean internal) {
        this.internal = internal;
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

    public ExchangeData addArgument(String key, Object value) {
        arguments.put(key, value);
        return this;
    }

    @Override
    public String toString() {
        return "ExchangeData{" +
                "type='" + type + '\'' +
                ", durable=" + durable +
                ", auto_delete=" + auto_delete +
                ", internal=" + internal +
                ", arguments=" + arguments +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ExchangeData that = (ExchangeData) o;

        if (durable != that.durable) return false;
        if (auto_delete != that.auto_delete) return false;
        if (internal != that.internal) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        return !(arguments != null ? !arguments.equals(that.arguments) : that.arguments != null);

    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (durable ? 1 : 0);
        result = 31 * result + (auto_delete ? 1 : 0);
        result = 31 * result + (internal ? 1 : 0);
        result = 31 * result + (arguments != null ? arguments.hashCode() : 0);
        return result;
    }
}
