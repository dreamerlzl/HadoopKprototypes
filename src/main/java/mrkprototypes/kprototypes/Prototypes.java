package mrkprototypes.kprototypes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

public class Prototypes {
	private static ArrayList<DataTuple> prototypes;
	public static ArrayList<DataTuple> getPrototypes(Configuration conf, int Iteration)
	throws IOException
	{
		String outputBase = conf.get("outputBase","/kprototypes");
		int k = conf.getInt("k", 0);
		int numberOfReducers = conf.getInt("mapreduce.job.reduces", 1);
		
		DataTuple.setNominalIndex(conf.getStrings("nominalIndex"));
		prototypes = new ArrayList<DataTuple>();
		FileSystem fs = FileSystem.get(conf);
		for(int i = 0; i < k;++i)
			prototypes.add(new DataTuple());
		for(int i = 0; i < numberOfReducers;++i)
		{
			Path pt = new Path(outputBase + "/tmp/output" + (Iteration) + "/prototypes-r-" + String.format("%05d", i));
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = br.readLine();
			while(line != null)
			{
				String[] tmps = line.split(",");
				int clusterId = Integer.parseInt(tmps[0]);
				String[] tmps2 = Arrays.copyOfRange(tmps,1, tmps.length);
				prototypes.set(clusterId, new DataTuple(String.join(",", tmps2)));
				line = br.readLine();
			}
		}
		return prototypes;
	}
	
	public static boolean convergeJudge(double e, Configuration conf, String outputBase, int currentIteration, double gamma)
	throws IOException
	{
		ArrayList<DataTuple> last_prototypes = getPrototypes(conf, currentIteration - 2);
		ArrayList<DataTuple> cur_prototypes = getPrototypes(conf,currentIteration - 1);
		for(int  i = 0 ; i < cur_prototypes.size();++i)
		{
			if(last_prototypes.get(i).metric(cur_prototypes.get(i), gamma) > e)
				return false;
		}
		return true;
	}
}
