package com.meltwater.puppy;

public class Main {
    public static void main(String[] argv) {
        if (!new Run().run("rabbit-puppy", argv)) {
            System.exit(1);
        }
    }
}


