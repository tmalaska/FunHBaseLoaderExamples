package com.cloudera.sa.hbaseloadexamples;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class HBasePutList {
  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      System.out.println("{inputDirector} {tableName} {columnFamily} {PutListSize}");
      return;
    }

    String inputFile = args[0];
    String tableName = args[1];
    byte[] columnFamily = Bytes.toBytes(args[2]);
    int putListSize = Integer.parseInt(args[3]);
    byte[] qualifier1 = Bytes.toBytes("C1");
    byte[] qualifier2 = Bytes.toBytes("C2");

    FileSystem fs = FileSystem.get(new Configuration());

    final Connection connection = ConnectionFactory.createConnection();
    Table table = connection.getTable(TableName.valueOf(tableName));
    int i = 0;
    try {
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
          table.put(putList);
          putList.clear();
        }

        if (i++ % 1000 == 0) {
          System.out.print(".");
        }
      }
      if (putList.size() > 0) {
        table.put(putList);
      }
      System.out.println("Finished: " + (System.currentTimeMillis() - startTime));
    } finally {
      table.close();
      connection.close();
    }

  }

  public static byte[] makeRowKey(String cell) {
    int hashMod = (Math.abs(cell.hashCode()) % 10);

    return Bytes.toBytes(hashMod + "|" + cell);
  }
}
