package org.example;

public class Main {
    public static void main(String[] args) throws Exception {
        new Producer().start();

//        new PollConsumer().start();
        new DefaultPushConsumer().start();

        Thread.sleep(100000000000000000L);
    }
}
