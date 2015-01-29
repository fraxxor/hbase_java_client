package demo.tasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import demo.DemoConfiguration;
import demo.Helper;
import demo.models.User;

public class PutUsersTask implements ITask {

	private Collection<User> users;

	public PutUsersTask(Collection<User> users) {
		this.users = users;
	}

	@Override
	public void doTask(DemoConfiguration config) throws IOException {

		System.out.println("Starting Usersputs. Amount: " + users.size());

		HTable table = new HTable(config.getConfiguration(), "users");
		int nextUuid = config.getNextUuid();

		List<Put> puts = new ArrayList<>(6 * users.size());
		for (User user : users) {
			String newuserid = Helper.getUuidStringFromNumber(nextUuid++);
			puts.add(new Put(Bytes.toBytes(newuserid)).add(
					Bytes.toBytes("logindata"), Bytes.toBytes("username"),
					Bytes.toBytes(user.getLogindata_username())));
			puts.add(new Put(Bytes.toBytes(newuserid)).add(
					Bytes.toBytes("logindata"), Bytes.toBytes("password"),
					Bytes.toBytes(user.getLogindata_password())));
			puts.add(new Put(Bytes.toBytes(newuserid)).add(
					Bytes.toBytes("contactdata"), Bytes.toBytes("firstname"),
					Bytes.toBytes(user.getContactdata_firstname())));
			puts.add(new Put(Bytes.toBytes(newuserid)).add(
					Bytes.toBytes("contactdata"), Bytes.toBytes("lastname"),
					Bytes.toBytes(user.getContactdata_lastname())));
			puts.add(new Put(Bytes.toBytes(newuserid)).add(
					Bytes.toBytes("contactdata"), Bytes.toBytes("email"),
					Bytes.toBytes(user.getContactdata_email())));
			puts.add(new Put(Bytes.toBytes(newuserid)).add(
					Bytes.toBytes("contactdata"), Bytes.toBytes("town"),
					Bytes.toBytes(user.getContactdata_town())));
		}
		table.put(puts);
		config.setNextUuid(nextUuid);

		System.out.println("User puts completed");

		table.close();

	}

}
