package com.dennyac.HbaseTest;

//cc GetExample Example application retrieving data from HBase
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

//import util.HBaseHelper;

public class GetExample {

	public static void main(String[] args) throws IOException {
		// vv GetExample
		System.out.println("Initializing");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "127.0.0.1");
		conf.set("hbase.zookeeper.property.clientPort", "2181");

		System.out.println("Configuration created");
		HTable table = new HTable(conf, "test");

		System.out.println("HTable object create");
		Get get = new Get(Bytes.toBytes("row1"));

		System.out.println("Creating get object");
		get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("a"));

		System.out.println("Adding column to get object");
		Result result = table.get(get);

		System.out.println("Assigning byte array");
		byte[] val = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("a"));

		System.out.println("Value: " + Bytes.toString(val));

		table.close();
	}
}
