package demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class DemoConfiguration {

	private Configuration config;
	private String configTableName;
	private int nextUuid = 0;

	public DemoConfiguration(Configuration origin, String configTableName) {
		this.config = origin;
		this.configTableName = configTableName;
		try {
			readConfigTable();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void readConfigTable() throws IOException {
		HTable table = new HTable(config, configTableName);

		Get get = new Get(Bytes.toBytes("config1"));
		get.addColumn(Bytes.toBytes("cfg"), Bytes.toBytes("next_uuid"));
		Result result = table.get(get);
		byte[] val = result.getValue(Bytes.toBytes("cfg"),
				Bytes.toBytes("next_uuid"));
		nextUuid = Integer.parseInt(Bytes.toString(val));

		table.close();
	}

	public Configuration getConfiguration() {
		return this.config;
	}

	public void setNextUuid(int nextUuid) {
		this.nextUuid = nextUuid;

		try {
			HTable table = new HTable(config, configTableName);
			Put put = new Put(Bytes.toBytes("config1"));
			put.add(Bytes.toBytes("cfg"), Bytes.toBytes("next_uuid"),
					Bytes.toBytes(nextUuid + ""));
			table.put(put);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public int getNextUuid() {
		return this.nextUuid;
	}

}
