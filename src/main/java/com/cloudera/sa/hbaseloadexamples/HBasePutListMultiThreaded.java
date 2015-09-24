package com.cloudera.sa.hbaseloadexamples;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HBasePutListMultiThreaded {
  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      System.out.println("{inputDirectory} {tableName} {columnFamily} {PutListSize} {threads}");
      return;
    }

    String inputFile = args[0];
    String tableName = args[1];
    byte[] columnFamily = Bytes.toBytes(args[2]);
    int putListSize = Integer.parseInt(args[3]);
    int numberOfThreads = Integer.parseInt(args[4]);

    ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

    byte[] qualifier1 = Bytes.toBytes("C1");
    byte[] qualifier2 = Bytes.toBytes("C2");

    FileSystem fs = FileSystem.get(new Configuration());

    int i = 0;

    long startTime = System.currentTimeMillis();
    ArrayList<Put> putList = new ArrayList();
    for (FileStatus fileStatus : fs.listStatus(new Path(inputFile))) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
      String[] cells = reader.readLine().split(",");

      Put put = new Put(makeRowKey(cells[0]));
      put.addColumn(columnFamily, qualifier1, Bytes.toBytes(cells[1]));
      put.addColumn(columnFamily, qualifier2, Bytes.toBytes(cells[2]));

      putList.add(put);

      if (putList.size() >= putListSize) {

        executor.execute(new PutListThread(TableName.valueOf(tableName), putList));

        putList = new ArrayList<Put>();
      }

      if (i++ % 1000 == 0) {
        System.out.print(".");
      }
    }
    if (putList.size() > 0) {
      executor.execute(new PutListThread(TableName.valueOf(tableName), putList));
    }
    System.out.println("Finished: " + (System.currentTimeMillis() - startTime));


  }

  public static byte[] makeRowKey(String cell) {
    int hashMod = (Math.abs(cell.hashCode()) % 10);

    return Bytes.toBytes(hashMod + "|" + cell);
  }
}

class PutListThread implements Runnable {

  TableName tableName;
  ArrayList<Put> putList = new ArrayList<Put>();

  public PutListThread(TableName tableName, ArrayList<Put> putList) {
    this.tableName = tableName;
    this.putList = putList;
  }

  @Override
  public void run() {
    Connection connection = null;
    try {
      connection = ConnectionFactory.createConnection();
      Table table = null;
      try {
        table = connection.getTable(tableName);
        table.put(putList);
      } finally {
        table.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

  }
}
