package aim3.hw1;

/*
 * The base of the code has been taken from the my team's implementation of
 * the Map Reduce Lab Exercise by Sergi Nadal
 * in the MSc Big Data Management course in UPC Barcelona
 * The team members were me (Ioannis Prapas) and Ankush Sharma
 */

import java.io.IOException;

public abstract class JobMapReduce {

	protected String input;
	protected String output;
	
	public void setInput(String input) {
		this.input = input;
	}
	
	public void setOutput(String output) {
		this.output = output;
	}
	
	public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		return false;
	}

}
