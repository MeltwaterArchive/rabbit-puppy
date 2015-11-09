package com.meltwater.puppy.config;

import java.util.HashMap;
import java.util.Map;

public class BindingData {
    private String destination;
    private String destination_type;
    private String routing_key;
    private Map<String, Object> arguments = new HashMap<>();

    public BindingData() {}

    public BindingData(String destination,
                       String destination_type,
                       String routing_key,
                       Map<String, Object> arguments) {
        this.destination = destination;
        this.destination_type = destination_type;
        this.routing_key = routing_key;
        setArguments(arguments);
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getRouting_key() {
        return routing_key;
    }

    public void setRouting_key(String routing_key) {
        this.routing_key = routing_key;
    }

    public String getDestination_type() {
        return destination_type;
    }

    public void setDestination_type(String destination_type) {
        this.destination_type = destination_type;
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

    @Override
    public String toString() {
        return "BindingData{" +
                "destination='" + destination + '\'' +
                ", destination_type='" + destination_type + '\'' +
                ", routing_key='" + routing_key + '\'' +
                ", arguments=" + arguments +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BindingData that = (BindingData) o;

        if (destination != null ? !destination.equals(that.destination) : that.destination != null) return false;
        if (destination_type != null ? !destination_type.equals(that.destination_type) : that.destination_type != null)
            return false;
        if (routing_key != null ? !routing_key.equals(that.routing_key) : that.routing_key != null) return false;
        return !(arguments != null ? !arguments.equals(that.arguments) : that.arguments != null);

    }

    @Override
    public int hashCode() {
        int result = destination != null ? destination.hashCode() : 0;
        result = 31 * result + (destination_type != null ? destination_type.hashCode() : 0);
        result = 31 * result + (routing_key != null ? routing_key.hashCode() : 0);
        result = 31 * result + (arguments != null ? arguments.hashCode() : 0);
        return result;
    }
}
