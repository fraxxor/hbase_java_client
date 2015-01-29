package com.dennyac.HbaseTest;

//cc GetExample Example application retrieving data from HBase
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
//import util.HBaseHelper;






import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PutMassive {

	public static void main(String[] args) throws IOException {		
		// vv GetExample
		System.out.println("Initializing");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "127.0.0.1");
		conf.set("hbase.zookeeper.property.clientPort", "2181");

		System.out.println("Configuration created");
		HTable table = new HTable(conf, "test"); 

		List<Put> puts = new ArrayList<>(1000);
		for (int i = 0; i < 1000; i++){
			Put put = new Put(Bytes.toBytes("rowMassive#" + i));
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("a"), Bytes.toBytes("#" + i + "#" + new Random().nextInt()));
			puts.add(put);
		}
		System.out.println("Start putting process of " + puts.size() + " objects");
		table.put(puts);
		System.out.println("Putting process completed");

		table.close();
	}
}
