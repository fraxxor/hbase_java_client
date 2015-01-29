package demo.tasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import demo.DemoConfiguration;
import demo.Helper;
import demo.models.User;

public class GetUsersRangeTask implements ITask {

	private int fromUuid, toUuid;
	private Collection<User> results = new ArrayList<>();

	public GetUsersRangeTask(int fromUuid, int toUuid) {
		this.fromUuid = fromUuid;
		this.toUuid = toUuid;
	}

	public Collection<User> getResults() {
		return this.results;
	}

	@Override
	public void doTask(DemoConfiguration config) throws IOException {
		System.out.printf("Getting table users in range from %s to %s\n",
				Helper.getUuidStringFromNumber(fromUuid),
				Helper.getUuidStringFromNumber(toUuid));

		List<Get> gets = new ArrayList<>();
		for (int i = fromUuid; i < toUuid; i++) {
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
			// Build User model object
			String username = Bytes.toString(res.getValue(
					Bytes.toBytes("logindata"), Bytes.toBytes("username")));
			String password = Bytes.toString(res.getValue(
					Bytes.toBytes("logindata"), Bytes.toBytes("password")));
			String firstname = Bytes.toString(res.getValue(
					Bytes.toBytes("contactdata"), Bytes.toBytes("firstname")));
			String lastname = Bytes.toString(res.getValue(
					Bytes.toBytes("contactdata"), Bytes.toBytes("lastname")));
			String email = Bytes.toString(res.getValue(
					Bytes.toBytes("contactdata"), Bytes.toBytes("email")));
			String town = Bytes.toString(res.getValue(
					Bytes.toBytes("contactdata"), Bytes.toBytes("town")));
			User user = new User(username, password);
			user.setContactdata_email(email);
			user.setContactdata_firstname(firstname);
			user.setContactdata_lastname(lastname);
			user.setContactdata_town(town);
			user.setUuid(Bytes.toString(res.getRow()));
			this.results.add(user);
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
