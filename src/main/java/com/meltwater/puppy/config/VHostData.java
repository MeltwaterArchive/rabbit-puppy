package com.meltwater.puppy.config;

public class VHostData {

    private boolean tracing = false;

    public VHostData() {
    }

    public VHostData(boolean tracing) {
        this.tracing = tracing;
    }

    public boolean isTracing() {
        return tracing;
    }

    public void setTracing(boolean tracing) {
        this.tracing = tracing;
    }

    @Override
    public String toString() {
        return "VHostData{" +
                "tracing=" + tracing +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VHostData vHostData = (VHostData) o;

        return tracing == vHostData.tracing;

    }

    @Override
    public int hashCode() {
        return (tracing ? 1 : 0);
    }
}
