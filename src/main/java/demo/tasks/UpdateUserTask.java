package demo.tasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import demo.DemoConfiguration;
import demo.models.User;

public class UpdateUserTask implements ITask {

	private User user;

	public UpdateUserTask(User user) {
		this.user = user;
		if (this.user.getUuid() == null) {
			throw new NullPointerException(
					"Field Uuid must not be NULL when updating a User");
		}
	}

	@Override
	public void doTask(DemoConfiguration config) throws IOException {

		System.out.println("Starting Userupdate. Amount: 1");

		HTable table = new HTable(config.getConfiguration(), "users");
		String userid = user.getUuid();

		List<Put> puts = new ArrayList<>(6);
		puts.add(new Put(Bytes.toBytes(userid)).add(Bytes.toBytes("logindata"),
				Bytes.toBytes("username"),
				Bytes.toBytes(user.getLogindata_username())));
		puts.add(new Put(Bytes.toBytes(userid)).add(Bytes.toBytes("logindata"),
				Bytes.toBytes("password"),
				Bytes.toBytes(user.getLogindata_password())));
		puts.add(new Put(Bytes.toBytes(userid)).add(
				Bytes.toBytes("contactdata"), Bytes.toBytes("firstname"),
				Bytes.toBytes(user.getContactdata_firstname())));
		puts.add(new Put(Bytes.toBytes(userid)).add(
				Bytes.toBytes("contactdata"), Bytes.toBytes("lastname"),
				Bytes.toBytes(user.getContactdata_lastname())));
		puts.add(new Put(Bytes.toBytes(userid)).add(
				Bytes.toBytes("contactdata"), Bytes.toBytes("email"),
				Bytes.toBytes(user.getContactdata_email())));
		puts.add(new Put(Bytes.toBytes(userid)).add(
				Bytes.toBytes("contactdata"), Bytes.toBytes("town"),
				Bytes.toBytes(user.getContactdata_town())));
		table.put(puts);

		System.out.println("User update completed");

		table.close();

	}

}
