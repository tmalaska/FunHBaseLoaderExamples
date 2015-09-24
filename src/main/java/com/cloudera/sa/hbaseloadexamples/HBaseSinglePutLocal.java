package com.cloudera.sa.hbaseloadexamples;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class HBaseSinglePutLocal {
  public static void main(String[] args) throws Exception {

    Logger.getRootLogger().setLevel(Level.ERROR);

    if (args.length == 0) {
      System.out.println("{inputDirector} {tableName} {columnFamily}");
      return;
    }

    String inputFile = args[0];
    String tableName = args[1];
    byte[] columnFamily = Bytes.toBytes(args[2]);
    byte[] qualifier1 = Bytes.toBytes("C1");
    byte[] qualifier2 = Bytes.toBytes("C2");

    FileSystem fs = FileSystem.get(new Configuration());

    HBaseTestingUtility htu = HBaseTestingUtility.createLocalHTU();

    htu.startMiniZKCluster();
    htu.startMiniHBaseCluster(1, 1);

    final Connection connection = ConnectionFactory.createConnection(htu.getConfiguration());

    //--

    Admin admin = connection.getAdmin();
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
    HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);

    //columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
    columnDescriptor.setBlocksize(64 * 1024);
    columnDescriptor.setBloomFilterType(BloomType.ROW);

    tableDesc.addFamily(columnDescriptor);

    byte[][] splitKeys = new byte[10][];
    splitKeys[0] = Bytes.toBytes("0");
    splitKeys[1] = Bytes.toBytes("1");
    splitKeys[2] = Bytes.toBytes("2");
    splitKeys[3] = Bytes.toBytes("3");
    splitKeys[4] = Bytes.toBytes("4");
    splitKeys[5] = Bytes.toBytes("5");
    splitKeys[6] = Bytes.toBytes("6");
    splitKeys[7] = Bytes.toBytes("7");
    splitKeys[8] = Bytes.toBytes("8");
    splitKeys[9] = Bytes.toBytes("9");

    admin.createTable(tableDesc, splitKeys);
    admin.close();

    //--

    Table table = connection.getTable(TableName.valueOf(tableName));
    int i = 0;
    try {
      long startTime = System.currentTimeMillis();
      for (FileStatus fileStatus : fs.listStatus(new Path(inputFile))) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));

        String line = reader.readLine();

        while (line != null) {
          String[] cells = line.split(",");

          Put put = new Put(makeRowKey(cells[0]));
          put.addColumn(columnFamily, qualifier1, Bytes.toBytes(cells[1]));
          put.addColumn(columnFamily, qualifier2, Bytes.toBytes(cells[2]));

          table.put(put);

          if (i++ % 10 == 0) {
            System.out.print(".");
          }
          line = reader.readLine();
        }
      }
      System.out.println("Finished: " + (System.currentTimeMillis() - startTime));
    } finally {
      table.close();
      connection.close();
    }

    htu.shutdownMiniHBaseCluster();
    htu.shutdownMiniZKCluster();

  }

  public static byte[] makeRowKey(String cell) {
    int hashMod = (Math.abs(cell.hashCode()) % 10);

    return Bytes.toBytes(hashMod + "|" + cell);
  }
}
