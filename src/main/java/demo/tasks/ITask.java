package demo.tasks;

import java.io.IOException;

import demo.DemoConfiguration;

public interface ITask {

	public void doTask(DemoConfiguration config) throws IOException;

}
