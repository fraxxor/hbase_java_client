package demo.tasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import demo.DemoConfiguration;

public class DeleteAllTask implements ITask {

	@Override
	public void doTask(DemoConfiguration config) throws IOException {
		HTable table = new HTable(config.getConfiguration(), "users");

		List<Delete> deletes = new ArrayList<>();
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		for (Result res : scanner) {
			Delete del = new Delete(res.getRow());
			deletes.add(del);
		}
		scanner.close();

		table.delete(deletes);

		config.setNextUuid(1);

		System.out.println("Delete all users completed");

		table.close();
	}

}
