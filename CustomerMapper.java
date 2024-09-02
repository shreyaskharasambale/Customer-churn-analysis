//First I will import all the Java and Hadoop Libraries that I will need in the Mapper Class
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class CustomerMapper extends Mapper<LongWritable, Text, Text, Text> {
//This line defines a class called CustomerMapper that extends the Mapper class. 
//It specifies the input and output key-value types for the Mapper as LongWritable, Text, Text, and Text respectively.  	
//This means that the Mapper takes LongWritable as input key, Text as input value, and produces Text as output key and value.
		
		@Override
//The "map" method is the main processing function of the Mapper class that is called for each input key-value pair.
	    protected void map(LongWritable key, Text value, Context c)	throws IOException, java.lang.InterruptedException

//The input key-value pair is passed as parameters (LongWritable key, Text value) to this method.
		
		{

		String line = value.toString();	
// the input value is converted from Text to String using the "toString()" method and stored in a local variable called "line".
				
		String[] words = line.split(",");
//The "line" variable is then split using the "," delimiter using the "split()" method, and the resulting substrings are stored in a String array called "words"
		
		String custID=  (words[0]); 
//The first element of the "words" array, i.e., "words[0]", is extracted and stored in a variable called "custID".		
		
		String data=   (words[1] + "," + words[8] + "," +words[9]);   
		
		c.write(new Text(custID), new Text(data));
// This line writes the custID and data as output key-value pair using the write() method on the Context object c.      
//The output key is created using new Text(custID) and the output value is created using new Text(data).		   
	    }
}
