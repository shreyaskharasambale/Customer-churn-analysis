import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CustomerReducer extends Reducer<Text, Text, Text, Text> {
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context c)throws IOException, java.lang.InterruptedException
//It takes a key-value pair as input, where the key is of type Text and represents the customer ID    
	{
                                             
	HashMap<Integer, Integer> OrdersEveryMonth = new HashMap<Integer, Integer>();   
//The reducer calculates various metrics, such as the number of orders every month using a HashMap<Integer, Integer> called 'OrdersEveryMonth'	                                        
	
	HashMap<String, Integer> FeedBacks = new HashMap<String, Integer>();      
//the count of feedback types using a HashMap<String, Integer> called FeedBacks.	
	
	Iterator<Text> valuesIter = values.iterator();
//This line creates an iterator to iterate over the input values.	
	
	while (valuesIter.hasNext())
	{
	    String f = valuesIter.next().toString();        
   	    String[] words = f.split(",");                 
//This loop iterates over the input values using the iterator and converts each value to a string. 
//Then, it splits the value using comma as the delimiter to extract individual words.   	    	    
		int Month = Integer.parseInt(words[0].split("-")[1].trim());           
	    int Ratings = Integer.parseInt(words[1].trim());                         
	    
	    String Feedback = words[2].trim();                                       
//These lines extract the month, rating, and feedback from the words array after parsing them to the appropriate data types (integer or string).	    
	   
	    // count number of orders per month 
	   
	    if (OrdersEveryMonth.containsKey(Month))
	    {
		OrdersEveryMonth.put(Month, OrdersEveryMonth.get(Month) + 1);
	    }
	    else
	    {
		OrdersEveryMonth.put(Month, 1);
//This code block updates the orderseveryMonth map to keep track of the number of orders per month. 
//If the month is already a key in the map, it increments the value by 1, otherwise it adds a new entry with the month as the key and the value set to 1.			    
		}
	    if (Ratings <= 3)
	    {
		// count type of feedback 
		if (FeedBacks.containsKey(Feedback))
		{
		    FeedBacks.put(Feedback, FeedBacks.get(Feedback) + 1);
		}
		else
		{
		    FeedBacks.put(Feedback, 1);
		}	    }
	    	}

	System.out.println("*****************************");
	System.out.println(key.toString());
	
	int LastMonthOrders = 0;                              
	int DeclineCount = 0;
	double OrderRate = 0;
	
	for (int i=6; i<=9; ++i)
	{                                                                
	    Integer ThisMonthOrders = OrdersEveryMonth.get(i);            
	                                                                  
	   	    
	    if (ThisMonthOrders == null)
		ThisMonthOrders = 0;

	    if (LastMonthOrders > 0)
		OrderRate = ((1.0*ThisMonthOrders)/LastMonthOrders)*100;           
	    
	    else
		OrderRate = 0;
	    // churn condition
//It calculates the order rate, which is the ratio of current month orders to previous month orders, 
//and checks for declining orders for three consecutive months with an order rate below 50%.			    
		if ((ThisMonthOrders < LastMonthOrders) &&(OrderRate <50.0))
	    {
		DeclineCount++;                                                     
	    }
	    
	    else
	    {
		DeclineCount = 0;
	    }

	    System.out.println(i + ", " + DeclineCount + ", " + OrderRate + ", " + ThisMonthOrders + ", " + LastMonthOrders);
	    
	    if (DeclineCount == 3)
	    {
		
		String OutFeedback = "";
		int FeedbackCount = 0;
		for (Map.Entry<String, Integer> e : FeedBacks.entrySet())
		{
		    if (e.getValue() > FeedbackCount)
		    {
			OutFeedback = e.getKey();              
			FeedbackCount = e.getValue();
		    }
		}
		c.write(key, new Text(OutFeedback));
		
	    }

	    LastMonthOrders = ThisMonthOrders;
	}
    }
}
