package com.dennyac.HbaseTest;

//cc GetExample Example application retrieving data from HBase
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
//import util.HBaseHelper;


import java.io.IOException;

public class PutExample {

	public static void main(String[] args) throws IOException {		
		// vv GetExample
		System.out.println("Initializing");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "127.0.0.1");
		conf.set("hbase.zookeeper.property.clientPort", "2181");

		System.out.println("Configuration created");
		HTable table = new HTable(conf, "test"); 


		System.out.println("HTable object create");
		Put put = new Put(Bytes.toBytes("rowPut")); 

		System.out.println("Creating put object");
		put.add(Bytes.toBytes("cf"), Bytes.toBytes("a"), Bytes.toBytes("fraxPutSomething123")); 

		System.out.println("Adding data to put object");
		table.put(put);

		System.out.println("Executed put request");

		table.close();
	}
}
