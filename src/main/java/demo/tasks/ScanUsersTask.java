package demo.tasks;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import demo.DemoConfiguration;

public class ScanUsersTask implements ITask {

	public ScanUsersTask() {

	}

	@Override
	public void doTask(DemoConfiguration config) throws IOException {
		System.out.println("Scanning table users");

		long timeStart = System.currentTimeMillis();

		HTable table = new HTable(config.getConfiguration(), "users");

		int amount = 0;
		String firstRowId = null;
		String lastRowId = null;
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		for (Result res : scanner) {
			amount++;
			if (firstRowId == null) {
				firstRowId = Bytes.toString(res.getRow());
			}
			lastRowId = Bytes.toString(res.getRow());
		}
		scanner.close();

		long timeNeeded = System.currentTimeMillis() - timeStart;

		System.out.println("Scan all users completed:");
		System.out.println("First Uuid: " + firstRowId);
		System.out.println("Last Uuid: " + lastRowId);
		System.out.println("Total amount of users: " + amount);
		System.out.printf("Time needed: %d ms\n", timeNeeded);

		table.close();
	}

}
