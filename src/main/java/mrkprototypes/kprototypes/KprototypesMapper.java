package mrkprototypes.kprototypes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class KprototypesMapper 
	extends Mapper<LongWritable, Text, IntWritable, DataTuple>
{
	private static ArrayList<DataTuple> prototypes;
	private static int currentIteration;
	private static int k;
	private static double gamma;
	private static int randomIndex;
	
	@Override
	public void setup(Context context)
	throws IOException
	{
		Configuration conf = context.getConfiguration();
		//read some iteration related information from job configuration
		gamma = conf.getDouble("gamma",1.0);
		currentIteration = conf.getInt("currentIteration",0);
		k = conf.getInt("k",0);
		String[] nominalIndex = conf.getStrings("nominalIndex");
		DataTuple.setNominalIndex(nominalIndex);
		String[] notUsed = conf.getStrings("notUsed");
		DataTuple.setNotUsed(notUsed);		
		//if this is not the first iteration, read the prototypes from last time output
		if(currentIteration > 0)
			prototypes =  Prototypes.getPrototypes(conf, currentIteration - 1);
		//random clustering for first iteration
		randomIndex = new Random().nextInt(k);
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException
	{
		DataTuple tuple = new DataTuple(value.toString());
		int minIndex = randomIndex % k;
		randomIndex = (randomIndex + 1) % k;
		//random clustering for first iteration
		if(currentIteration > 0)
		{ 
			double minMetric = Double.MAX_VALUE;
			for(int i = 0; i < k;++i)
			{	
				double tempMetric = tuple.metric(prototypes.get(i), gamma);
				if(tempMetric < minMetric)
				{
					minIndex = i;
					minMetric = tempMetric;
				}
			}
		}
		context.write(new IntWritable(minIndex),tuple);
	}
}
