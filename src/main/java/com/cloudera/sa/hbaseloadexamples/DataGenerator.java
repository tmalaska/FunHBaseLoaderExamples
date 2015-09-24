package com.cloudera.sa.hbaseloadexamples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.ArrayList;

public class DataGenerator {
	
  public static void main(String[] args) throws IOException {
    if(args.length == 0) {
      System.out.println("{outputDir} {NumberOfRecords} {numberOfThreads}");
      return;
    }

    String outputDirectory = args[0];
    int numberOfRecords = Integer.parseInt(args[1]);
    int numberOfThreads = Integer.parseInt(args[2]);
    int numberOfRecordsPerThread = numberOfRecords/numberOfThreads;

    FileSystem fs = FileSystem.get(new Configuration());

    ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

    ArrayList<Future> futureList = new ArrayList();

    for (int i = 0; i < numberOfThreads; i++) {
      WriterThread thread = new WriterThread(outputDirectory + "/File" + i + ".txt", fs, numberOfRecordsPerThread);
      futureList.add(executor.submit(thread));
    }

    for (Future future: futureList) {
      try {
        future.get();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }
    
    executor.shutdown();
  }
  
}

class WriterThread implements Runnable {

	String outputFileName;
	FileSystem fs;
	int numberOfRecordsToWrite;

	public WriterThread(String outputFileName, FileSystem fs, int numberOfRecordsToWrite) {
		this.outputFileName = outputFileName;
		this.fs = fs;
		this.numberOfRecordsToWrite = numberOfRecordsToWrite;
	}

	  @Override
	  public void run() {
	    BufferedWriter writer = null;
	    Random r = new Random();
	    try {
	      writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outputFileName))));

	      for (int i = 0; i < numberOfRecordsToWrite; i++) {
	        writer.write("foo" + i + "," + r.nextInt(100) + "," + r.nextInt());
	        writer.newLine();
	        
	        if (i % 1000 == 0) {
	            System.out.print(".");
	        }
	      }

	    } catch (IOException e) {
	      e.printStackTrace();
	    } finally {
	      if (writer != null) {
	        try {
	          writer.close();
	        } catch (IOException e) {
	          e.printStackTrace();
	        }
	      }
	    }
	  }
}

