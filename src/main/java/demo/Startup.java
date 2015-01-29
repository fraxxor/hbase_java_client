package demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import demo.models.User;
import demo.tasks.DeleteAllTask;
import demo.tasks.GetUsersRangeTask;
import demo.tasks.GetUsersTask;
import demo.tasks.PutUsersTask;
import demo.tasks.ScanUsersRangeTask;
import demo.tasks.ScanUsersTask;
import demo.tasks.UpdateUsersTask;

public class Startup {

	private static Logger logger = Logger.getLogger(Startup.class);
	private static final String usage = "Usage: Type one of the following\n"
			+ "putRandomUsers1000 ~ putRandomUsers10000 ~ putRandomUsers100000 ~ putRandomUsers1M ~ scanAllUsers ~ getAllUsers ~ scanUsers #from #to ~ getUsers #from #to ~ updateUsers #from #to ~ deleteAll ~ exit";

	public static void main(String[] args) {
		setLogLeve();
		new Startup();
	}

	private static void setLogLeve() {
		@SuppressWarnings("unchecked")
		List<Logger> loggers = Collections.<Logger> list(LogManager
				.getCurrentLoggers());
		loggers.add(LogManager.getRootLogger());
		for (Logger logger : loggers) {
			logger.setLevel(Level.ERROR);
		}
		Startup.logger.setLevel(Level.INFO);
	}

	private DemoConfiguration config;

	private Startup() {
		init();
		startCommandInput();
	}

	private void init() {
		System.out.println("Initializing");
		Configuration hbconfig = HBaseConfiguration.create();
		hbconfig.set("hbase.zookeeper.quorum", "127.0.0.1");
		hbconfig.set("hbase.zookeeper.property.clientPort", "2181");
		this.config = new DemoConfiguration(hbconfig, "configuration_demo");
		System.out.println("Configuration created");
	}

	private void startCommandInput() {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(
				System.in))) {
			System.out.println("Type a command to proceed");
			System.out.println(usage);
			String command = br.readLine();
			while (command != null) {
				issueCommand(command);
				command = br.readLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void issueCommand(String commandLine) {
		logger.info("Command issued: " + commandLine);
		String[] commands = commandLine.split(" ");
		String command = commands[0];
		boolean commandNotFound = false;
		switch (command) {
		case "putRandomUsers1000":
			putRandomUsers1000();
			break;
		case "putRandomUsers10000":
			putRandomUsers10000();
			break;
		case "putRandomUsers100000":
			putRandomUsers100000();
			break;
		case "putRandomUsers1M":
			putRandomUsers1M();
			break;
		case "scanAllUsers":
			scanAllUsers();
			break;
		case "getAllUsers":
			getAllUsers();
			break;
		case "scanUsers":
			if (commands.length > 2) {
				scanUsersInRange(commands[1], commands[2]);
			} else {
				System.out.println("Missing arguments");
			}
			break;
		case "getUsers":
			if (commands.length > 2) {
				getUsersInRange(commands[1], commands[2]);
			} else {
				System.out.println("Missing arguments");
			}
			break;
		case "updateUsers":
			if (commands.length > 2) {
				updateUsersInRange(commands[1], commands[2]);
			} else {
				System.out.println("Missing arguments");
			}
			break;
		case "deleteAll":
			deleteAll();
			break;
		case "exit":
			System.exit(0);
			break;
		default:
			commandNotFound = true;
			System.out.println("No such command");
			System.out.println(usage);
		}
		if (!commandNotFound) {
			logger.info("Issued task completed: " + command);
		}
	}

	private void putRandomUsers1000() {
		Collection<User> users = new ArrayList<User>();
		NameGenerator gen = new NameGenerator();
		Random rdm = new Random();
		for (int i = 0; i < 1000; i++) {
			String username = gen.getName();
			User u = new User(username, gen.getName() + rdm.nextInt());
			u.setContactdata_email(username + "@mail.foo");
			u.setContactdata_firstname(gen.getName());
			u.setContactdata_lastname(gen.getName());
			u.setContactdata_town("Imaginary_Town");
			users.add(u);
		}
		try {
			new PutUsersTask(users).doTask(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void putRandomUsers10000() {
		Collection<User> users = new ArrayList<User>();
		NameGenerator gen = new NameGenerator();
		Random rdm = new Random();
		for (int i = 0; i < 10000; i++) {
			String username = gen.getName();
			User u = new User(username, gen.getName() + rdm.nextInt());
			u.setContactdata_email(username + "@mail.foo");
			u.setContactdata_firstname(gen.getName());
			u.setContactdata_lastname(gen.getName());
			u.setContactdata_town("Imaginary_Town");
			users.add(u);
		}
		try {
			new PutUsersTask(users).doTask(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void putRandomUsers100000() {
		Collection<User> users = new ArrayList<User>();
		NameGenerator gen = new NameGenerator();
		Random rdm = new Random();
		for (int i = 0; i < 100000; i++) {
			String username = gen.getName();
			User u = new User(username, gen.getName() + rdm.nextInt());
			u.setContactdata_email(username + "@mail.foo");
			u.setContactdata_firstname(gen.getName());
			u.setContactdata_lastname(gen.getName());
			u.setContactdata_town("Imaginary_Town");
			users.add(u);
		}
		try {
			new PutUsersTask(users).doTask(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void putRandomUsers1M() {
		NameGenerator gen = new NameGenerator();
		Random rdm = new Random();
		for (int k = 0; k < 10; k++) {
			Collection<User> users = new ArrayList<User>();
			for (int i = 0; i < 100000; i++) {
				String username = gen.getName();
				User u = new User(username, gen.getName() + rdm.nextInt());
				u.setContactdata_email(username + "@mail.foo");
				u.setContactdata_firstname(gen.getName());
				u.setContactdata_lastname(gen.getName());
				u.setContactdata_town("Imaginary_Town");
				users.add(u);
			}
			try {
				new PutUsersTask(users).doTask(config);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void scanAllUsers() {
		try {
			new ScanUsersTask().doTask(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void getAllUsers() {
		try {
			new GetUsersTask().doTask(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void scanUsersInRange(String from, String end) {
		try {
			new ScanUsersRangeTask(from, end).doTask(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void getUsersInRange(String from, String end) {
		try {
			new GetUsersRangeTask(Helper.getNumberFromUuidString(from),
					Helper.getNumberFromUuidString(end)).doTask(config);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NumberFormatException nfe) {
			System.out.println("Argument not a number");
		}
	}

	private void updateUsersInRange(String from, String end) {
		NameGenerator gen = new NameGenerator();
		// Random rdm = new Random();
		try {
			ScanUsersRangeTask scanTask = new ScanUsersRangeTask(from, end);
			scanTask.doTask(config);

			Collection<User> users = scanTask.getResults();
			for (User user : users) {
				String username = gen.getName();
				user.setLogindata_username(username);
				user.setContactdata_email(username + "@mail.foo");
				user.setContactdata_firstname(gen.getName());
				user.setContactdata_lastname(gen.getName());
			}
			new UpdateUsersTask(users).doTask(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void deleteAll() {
		try {
			new DeleteAllTask().doTask(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}