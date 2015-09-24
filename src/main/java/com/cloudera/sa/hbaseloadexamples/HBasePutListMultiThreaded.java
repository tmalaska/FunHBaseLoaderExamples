package com.cloudera.sa.hbaseloadexamples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class HBasePutListMultiThreaded
{
	public static AtomicInteger threadFinishedCounter = new AtomicInteger(0);
	
	protected static List<Connection> _connectionsAvailable;
	protected static List<Connection> _connectionsInUse;

	public static void main(String[] args) throws IOException
	{
		Logger.getRootLogger().setLevel(Level.WARN);

		String usage = "{inputDirectory} {tableName} {columnFamily} {PutListSize} {threads}";
		if (args.length != usage.split(" ").length)
		{
			System.out.println(usage);
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
		
		int threadsStarted = 0;
		
		ArrayList<Put> putList = new ArrayList<Put>();
		for (FileStatus fileStatus : fs.listStatus(new Path(inputFile)))
		{
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));

			String line = null;
			while ((line = reader.readLine()) != null)
			{
				String[] cells = line.split(",");

				Put put = new Put(makeRowKey(cells[0]));
				put.addColumn(columnFamily, qualifier1, Bytes.toBytes(cells[1]));
				put.addColumn(columnFamily, qualifier2, Bytes.toBytes(cells[2]));

				putList.add(put);

				if (putList.size() >= putListSize)
				{

					threadsStarted++;
					executor.execute(new PutListThread(TableName.valueOf(tableName), putList));

					putList = new ArrayList<Put>();
				}

				if (i++ % 1000 == 0)
				{
					System.out.print(".");
				}
			}
		}
		if (putList.size() > 0)
		{
			threadsStarted++;
			executor.execute(new PutListThread(TableName.valueOf(tableName), putList));
		}

		executor.shutdown();
		Connection connection = null;
		while ((connection = getConnection()) != null)
		{
			connection.close();
		}

		System.out.println("\nFinished: " + (System.currentTimeMillis() - startTime) + " " + HBasePutListMultiThreaded.threadFinishedCounter.get() + " " + threadsStarted);
	}
	
	public static synchronized Connection getConnection()
	{
		Connection connection = null;
		
		if (_connectionsAvailable.isEmpty()) 
		{
			try
			{
				connection = ConnectionFactory.createConnection();
			} 
			catch (IOException e)
			{
				e.printStackTrace();
			}
			_connectionsInUse.add(connection);
		}
		else
		{
			connection = _connectionsAvailable.get(0);
			_connectionsInUse.add(connection);
		}
		return connection;
	}
	
	public static synchronized void putConnection(Connection connection)
	{
		_connectionsInUse.remove(connection);
		_connectionsAvailable.add(connection);
	}

	public static byte[] makeRowKey(String cell)
	{
		int hashMod = (Math.abs(cell.hashCode()) % 10);

		return Bytes.toBytes(hashMod + "|" + cell);
	}
}

class PutListThread implements Runnable
{

	TableName tableName;
	ArrayList<Put> putList = new ArrayList<Put>();

	public PutListThread(TableName tableName, ArrayList<Put> putList)
	{
		this.tableName = tableName;
		this.putList = putList;
	}

	@Override
	public void run()
	{
		Connection connection = null;
		try
		{
			connection = HBasePutListMultiThreaded.getConnection();
			Table table = null;
			try
			{
				table = connection.getTable(tableName);
				table.put(putList);
			} finally
			{
				table.close();
			}
		} 
		catch (IOException e)
		{
			e.printStackTrace();
		} 
		finally
		{
			if (connection != null)
			{
				HBasePutListMultiThreaded.putConnection(connection);
			}
		}
		HBasePutListMultiThreaded.threadFinishedCounter.incrementAndGet();
	}
	
}
