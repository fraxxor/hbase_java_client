package demo.tasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import demo.DemoConfiguration;
import demo.Helper;

public class GetUsersTask implements ITask {

	@Override
	public void doTask(DemoConfiguration config) throws IOException {
		System.out.println("Get table users");

		List<Get> gets = new ArrayList<>();
		for (int i = 0; i < config.getNextUuid(); i++) {
			Get get = new Get(Bytes.toBytes(Helper.getUuidStringFromNumber(i)));
			gets.add(get);
		}

		long timeStart = System.currentTimeMillis();

		HTable table = new HTable(config.getConfiguration(), "users");

		int amount = 0;
		String firstRowId = null;
		String lastRowId = null;
		Result[] results = table.get(gets);
		for (int i = 1; i < results.length; i++) {
			Result res = results[i];
			amount++;
			if (firstRowId == null) {
				firstRowId = Bytes.toString(res.getRow());
			}
			lastRowId = Bytes.toString(res.getRow());
		}

		long timeNeeded = System.currentTimeMillis() - timeStart;

		System.out.println("Scan all users completed:");
		System.out.println("First Uuid: " + firstRowId);
		System.out.println("Last Uuid: " + lastRowId);
		System.out.println("Total amount of users: " + amount);
		System.out.printf("Time needed: %d ms\n", timeNeeded);

		table.close();
	}

}
