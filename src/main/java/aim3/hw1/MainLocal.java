package aim3.hw1;

/*
 * The base of the code has been taken from the my team's implementation of
 * the Map Reduce Lab Exercise by Sergi Nadal
 * in the MSc Big Data Management course in UPC Barcelona
 * The team members were me (Ioannis Prapas) and Ankush Sharma
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MainLocal extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LocalMapReduce");


        if (args[0].equals("-ex1")) {
            ex1.configureJob(job, args[1],args[2]);
        }

        else if(args[0].equalsIgnoreCase("-ex2")) {
            ex2.configureJob(job, args[1], args[2], args[3]);
        }

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    private static String[] getPathsIn(final String[] args) {
        //I am just using Arraylist and going to convert this to an String[] later
        final String[] pathInsList = new String[args.length];
        for (int i = 1 ; i < args.length - 1 ; i ++) {
            pathInsList[i] = args[i];
        }
        return pathInsList;
    }

    public static void main(String[] args) throws Exception {
        MainLocal driver = new MainLocal();
        int exitCode = ToolRunner.run(driver,args);
        System.exit(exitCode);

    }

}
