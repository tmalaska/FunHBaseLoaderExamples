package com.cloudera.sa.hbaseloadexamples;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by ted.malaska on 9/24/15.
 */
public class ThreadTest {
  public static void main(String[] args) {
    ThreadPoolExecutor executorPool = new ThreadPoolExecutor(2, 4, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(10));

    for (int i = 0; i < 100; i++) {
      System.out.println("-" + i);
      
      executorPool.execute(new WaitRunnable());
    }

    System.out.println("---");
    //executorPool.shutdown();
    System.out.println("----");
  }
}

class WaitRunnable implements Runnable {

  @Override
  public void run() {
    System.out.println("<");
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    System.out.println(">");
  }
}