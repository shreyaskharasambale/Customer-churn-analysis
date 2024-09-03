# Customer-churn-analysis
Customer attrition, commonly referred to as customer churn, is the occurrence of customers abandoning their association or 
contact with a firm or corporation. It happens when clients cease utilizing a business's goods or services or terminate their 
memberships. Any organization wants to reduce customer turnover since it's usually more expensive to get new customers than 
it is to keep the ones you already have.  
An online food delivery service needs to identify which of its clients is most likely to stop placing new orders in the near future. 
The promotional team of the business can develop ways to keep these clients by using this information. 
Data: 
This dataset was obtained from the Kaggle website  
Link for the Dataset: https://www.kaggle.com/datasets/shreyaskharasambale/customer-churn-dataset 
The Dataset comprises of six features, including customer id, order date, food items, restaurant, customer rating, and feedback. 
The Data column name and its description is given in the table below. For our analysis, we have the data for 2 months (August – 
September) in a .csv format 

I’m going to use Map-reduce for addressing the two questions. 
Map Function: Data input is transformed into intermediate key-value pairs using the Map function. It is in charge of 
processing each input record separately and producing interim findings. 

Reduce Function: The Map function's output is passed to the Reduce function, which organises it by key and then does 
additional processing to produce the desired result. According to the specifications of the problem, it is in charge of collecting, 
summarising, or processing the intermediate outcomes. 

Driver code: The primary programme or script that manages the coordination of the Map and Reduce function execution in a 
MapReduce architecture is known as the driver code. It often configures the input/output pathways, additional parameters, and 
configuration needed for the MapReduce job. The driver code also handles input and output data, calls Map and Reduce routines, 
and keeps track of how the process is going. 
