/*=============================================================================
|   Assignment:  Individual assignment: Programming - 3
|       Author:  Sampath Kumar Gunasekaran(sgunase2@uncc.edu)
|       Grader:  Walid Shalaby
|
|       Course:  ITCS 6190
|   Instructor:  Srinivas Akella
|     Due Date:  March 23 at 11:59PM
|
|     Language:  Java 
|     Version :  1.8.0_101
|                
| Deficiencies:  No logical errors.
 *===========================================================================*/
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * InvertedIndexJob class includes main and run methods, and the inner classes
 * Map and Reduce. It outputs the locations and the frequency of words in a
 * particular page for each distinct word in the body of the wiki pages and
 * saves the results to the output location in HDFS.
 * 
 * @author Sampath Kumar
 * @version 1.0
 * @since 2017-03-18
 */

public class InvertedIndexJob extends Configured implements Tool {

	/*
	 * The main method invokes ToolRunner, which creates and runs a new instance
	 * of InvertedIndexJob, passing the command line arguments
	 */
	public static void main(String[] args) throws Exception {
		int invIndex = ToolRunner.run(new InvertedIndexJob(), args);
		System.exit(invIndex);
	}

	/*
	 * The run method configures the job, starts the job, waits for the job to
	 * complete, and then returns an integer value as the success flag.
	 */
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "InvertedIndexJob");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.setInputFormatClass(XmlInputFormat.class); // setting the input format of the document as Xml format.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
