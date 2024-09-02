import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Customer{
//This declares the Customer class, which serves as the entry point for the MapReduce program.

	public static void main(String[] args) throws Exception {
        
        // Create a new job and configure it
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Customer Analysis");
        job.setJarByClass(Customer.class);
        job.setMapperClass(CustomerMapper.class);
        job.setReducerClass(CustomerReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//This block of code creates a new Job object, which represents the MapReduce job to be executed. 
//It configures various properties of the job, such as the jar file containing the program (Customer.class),
// the mapper class (CustomerMapper.class), the reducer class (CustomerReducer.class), and the input/output key-value classes (Text.class).
        
        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[1]));
//The input path is set using FileInputFormat.addInputPath() method, which takes a Path object representing the input directory as an argument.        
        
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
//The output path is set using FileOutputFormat.setOutputPath() method, which takes a Path object representing the output directory as an argument.
        
        
        // Submit the job and wait for completion
        job.waitForCompletion(true);
//This line submits the MapReduce job for execution and waits for its completion. The true argument indicates that the job's progress should be printed to the console during execution. Once the job is completed, the program exits.
        
    }

}
