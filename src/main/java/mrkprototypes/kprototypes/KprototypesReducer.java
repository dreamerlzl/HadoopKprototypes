package mrkprototypes.kprototypes;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
//import com.google.common.collect.Iterables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.Iterator;

public class KprototypesReducer 
	extends Reducer<IntWritable, DataTuple, Text, Text>
{
	private  MultipleOutputs<Text, Text> moutput;
	private  int currentIteration;
	private  int maxIteration;
	
	@Override
	public void setup(Context context)
	{
		Configuration conf = context.getConfiguration();
		//read some iteration related information from job configuration
		currentIteration = conf.getInt("currentIteration",0);
		maxIteration = conf.getInt("maxIteration",0);
		DataTuple.setNominalIndex(conf.getStrings("nominalIndex"));
		DataTuple.setNotUsed(conf.getStrings("notUsed"));
		moutput = new MultipleOutputs<Text, Text>(context);
	}
	
	@Override
	public void reduce(IntWritable key, Iterable<DataTuple> values, Context context)
	throws IOException, InterruptedException
	{
		if(currentIteration > 0 && currentIteration < maxIteration)
		{
			/*if this is not the first iteration, 
			 * figure out the modes for nominal features 
			 * and means for numeric features */ 
			DataTuple first = values.iterator().next();
			int numberOfNumerics = first.getNumeric().size();
			int numberOfNominals = first.getNominal().size();
			Map<Integer,Map<String,Integer>> nominalCount = new HashMap<Integer, Map<String, Integer>>();
			for(int i = 0; i < numberOfNominals;++i)
				nominalCount.put(i, new HashMap<String,Integer>());
			//figure out the new centroids for numerics and the new modes for nominals
			Double[] centroid = new Double[numberOfNumerics];
			String[] mode = new String[numberOfNominals];
			for(int i = 0; i < numberOfNumerics;++i)
				centroid[i] = 0.0;
			int numberOfTuples = 0;
			for(DataTuple value : values)
			{
				context.write(new Text(key.toString()), new Text("," + value.getOutput()));
				++numberOfTuples;
				ArrayList<Double> numeric = value.getNumeric();
				ArrayList<String> nominal = value.getNominal();
				for(int i = 0;i <  numberOfNumerics;++i)
				{
					centroid[i] += numeric.get(i);
				}
				for(int i = 0;i < numberOfNominals;++i)
				{
					String cnominal = nominal.get(i);
					if(nominalCount.get(i).containsKey(cnominal))
					{
						int temp = nominalCount.get(i).get(cnominal);
						nominalCount.get(i).put(cnominal, temp + 1);
					}
					else nominalCount.get(i).put(nominal.get(i), 0);
				}
			}

			for(int i = 0; i < numberOfNumerics;++i)
				centroid[i] /= numberOfTuples;
					
			for(int i = 0; i < numberOfNominals;++i)
			{
				Map<String,Integer> tmap = nominalCount.get(i);
				Set<String> keys = tmap.keySet();
				int maxCount = 0; String tmode = "";
				for(String s : keys)
				{
					if(tmap.get(s) > maxCount)
					{
						maxCount = tmap.get(s);
						tmode = s;
					}
				}
				mode[i] = tmode;
			}
			moutput.write("prototypes",new Text(key.toString() + ","), new Text(DataTuple.inverse(mode,centroid)));
		}
		else if(currentIteration == maxIteration)
		{
			//in the last iteration, there is no need to refresh prototypes
			for(DataTuple value : values)
			{
				context.write(new Text(key.toString() + ","), new Text(value.getOutput()));
			}
		}
		else
		{
			//in the first iteration, randomly pick k tuples as prototypes
			Iterator<DataTuple> itr = values.iterator();
			moutput.write("prototypes", new Text(key.toString() + ","), new Text(itr.next().getOutput()));
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		moutput.close();
	}
}
