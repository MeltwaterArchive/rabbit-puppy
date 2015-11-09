package com.meltwater.puppy.config;

public class PermissionsData {
    private String configure = ".*";
    private String write = ".*";
    private String read = ".*";

    public PermissionsData() {}

    public PermissionsData(String configure, String write, String read) {
        this.configure = configure;
        this.write = write;
        this.read = read;
    }

    public String getConfigure() {
        return configure;
    }

    public void setConfigure(String configure) {
        this.configure = configure;
    }

    public String getWrite() {
        return write;
    }

    public void setWrite(String write) {
        this.write = write;
    }

    public String getRead() {
        return read;
    }

    public void setRead(String read) {
        this.read = read;
    }

    @Override
    public String toString() {
        return "PermissionsData{" +
                "configure='" + configure + '\'' +
                ", write='" + write + '\'' +
                ", read='" + read + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PermissionsData that = (PermissionsData) o;

        if (configure != null ? !configure.equals(that.configure) : that.configure != null) return false;
        if (write != null ? !write.equals(that.write) : that.write != null) return false;
        return !(read != null ? !read.equals(that.read) : that.read != null);

    }

    @Override
    public int hashCode() {
        int result = configure != null ? configure.hashCode() : 0;
        result = 31 * result + (write != null ? write.hashCode() : 0);
        result = 31 * result + (read != null ? read.hashCode() : 0);
        return result;
    }
}
