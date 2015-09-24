package com.cloudera.sa.hbaseloadexamples;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by ted.malaska on 9/24/15.
 */
public class CreateTable {
  public static void main(String[] args) throws IOException {

    if (args.length == 0) {
      System.out.println("{inputDirector} {tableName} {columnFamily}");
      return;
    }

    String tableName = args[0];
    byte[] columnFamily = Bytes.toBytes(args[1]);

    final Connection connection = ConnectionFactory.createConnection();
    Admin admin = connection.getAdmin();
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
    HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);

    columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
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
  }
}
