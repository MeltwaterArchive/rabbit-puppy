package com.meltwater.puppy.config;

public class UserData {

    private String password;
    private boolean admin = false;

    public UserData() {}

    public UserData(String password, boolean admin) {
        this.password = password;
        this.admin = admin;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isAdmin() {
        return admin;
    }

    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    @Override
    public String toString() {
        return "UserData{" +
                "password='" + password + '\'' +
                ", admin=" + admin +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UserData userData = (UserData) o;

        if (admin != userData.admin) return false;
        return !(password != null ? !password.equals(userData.password) : userData.password != null);

    }

    @Override
    public int hashCode() {
        int result = password != null ? password.hashCode() : 0;
        result = 31 * result + (admin ? 1 : 0);
        return result;
    }
}
