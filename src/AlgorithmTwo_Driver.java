import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Michael Oertel and Aldo D'Eramo on 03/06/16.
 */
public class AlgorithmTwo_Driver extends Configured implements Tool {

	static int printUsage() {
		System.out.println("AlgorithmTwo_Driver [-m <maps>] [-r <reduces>] <input> <temp_1> <temp_2> <output> <stripes/pairs>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Method Name: main Return type: none Purpose:Read the arguments from
	 * command line and run the Job till completion
	 */
	public static void main(String[] args) throws Exception {

		long startTime = System.currentTimeMillis();

		ToolRunner.run(new Configuration(), new AlgorithmTwo_Driver(), args);

		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("Elapsed time: " + totalTime / 1000 + " seconds");
	}

	public static void deleteFolder(File folder) {
		File[] files = folder.listFiles();
		if (files != null) { // some JVMs return null for empty dirs
			for (File f : files) {
				if (f.isDirectory()) {
					deleteFolder(f);
				} else {
					f.delete();
				}
			}
		}
		folder.delete();
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		List<String> otherArgs = new ArrayList<String>();

		/* Read arguments */
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					conf.setInt("mapreduce.job.maps", Integer.parseInt(args[++i]));
				} else if ("-r".equals(args[i])) {
					conf.setInt("mapreduce.job.reduces", Integer.parseInt(args[++i]));
				} else {
					otherArgs.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				System.exit(printUsage());
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " + args[i - 1]);
				System.exit(printUsage());
			}
		}
		// Make sure there are exactly 4 parameters left.
		if (otherArgs.size() != 5) {
			System.out.println("ERROR: Wrong number of parameters: " + otherArgs.size() + " instead of 5.");
			System.exit(printUsage());
		}

		File tmp1 = new File(otherArgs.get(1));
		File tmp2 = new File(otherArgs.get(2));
		File out = new File(otherArgs.get(3));

		if (tmp2.exists() && tmp2.isDirectory()) {
			deleteFolder(tmp2);
		} else { System.err.println("WARN: temp_2 folder \""+tmp2+"\" doesn't exists");}
		if (out.exists() && out.isDirectory()) {
			deleteFolder(out);
		} else { System.err.println("WARN: output folder \""+out+"\" doesn't exists");}
		if (tmp1.exists() && tmp1.isDirectory()) {
			File[] files = tmp1.listFiles();
			if (files.length == 0) { // some JVMs return null for empty dirs
				System.err.println("ERROR: temp_1 folder \""+tmp1+"\" is empty");
				System.exit(0);
			}
		} else { System.err.println("ERROR: temp_1 folder \""+tmp1+"\" doesn't exists"); }

/*
 * Round 1
 */
		FileSystem fs = FileSystem.get(conf);
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(AlgorithmTwo_Driver.class);

	/*
	 * Approccio Stripes
	 */
		if (otherArgs.get(4).equals("stripes")) {
			job1.setMapperClass(Mapper_1_stripes.class);
			job1.setCombinerClass(Combiner_1.class);
			job1.setReducerClass(Reducer_1_stripes.class);
			
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(MapWritable.class);
			
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
		}
	/*
	 * Approccio Pairs
	 */
		if (otherArgs.get(4).equals("pairs")) {
			job1.setMapperClass(Mapper_1_pairs.class);
			job1.setCombinerClass(Reducer_1_pairs.class);
			job1.setReducerClass(Reducer_1_pairs.class);
	
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(IntWritable.class);
	
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
		}
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(job1, new Path(otherArgs.get(0)));
		TextOutputFormat.setOutputPath(job1, new Path(otherArgs.get(2)));

		job1.waitForCompletion(true);

/*
 * Round 2
 */
		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(AlgorithmTwo_Driver.class);

		job2.setMapperClass(Mapper_2.class);
		job2.setReducerClass(Reducer_2.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(job2, new Path(otherArgs.get(1)));
		TextInputFormat.addInputPath(job2, new Path(otherArgs.get(2)));
		TextOutputFormat.setOutputPath(job2, new Path(otherArgs.get(3)));

		return job2.waitForCompletion(true) ? 0 : 1;
	}
}
