package com.dennyac.HbaseTest;

//cc GetExample Example application retrieving data from HBase
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
//import util.HBaseHelper;


import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ScanExample {

	public static void main(String[] args) throws IOException {

		System.out.println("Initializing");
		Configuration conf = HBaseConfiguration.create();
		
		System.out.println("Created Configuration");
		conf.set("hbase.zookeeper.quorum", "192.168.1.101");
		conf.set("hbase.zookeeper.property.clientPort", "2181");

		System.out.println("Configuration created");
		HTable table = new HTable(conf, "test");
		System.out.println("HTable instantiated");
		
		Scan scan = new Scan(Bytes.toBytes("row1"), Bytes.toBytes("row3"));
		System.out.println("Scan object instantiated");
		ResultScanner scanner = table.getScanner(scan);
		System.out.println("ResultScanner object instantiated");
		for(Result res: scanner){
			System.out.println(Bytes.toString(res.getRow()));
			System.out.println("Next iteration");
		}
		
		scanner.close();
		System.out.println("Closed scanner");
		table.close();
		System.out.println("Closed table");
	}
}
