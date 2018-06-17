package mrkprototypes.kprototypes;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class KprototypesDriver {
	private static int currentIteration;
	private static int maxIteration;
	private static String inputPath;
	private static String outputPath;
	private static int numberOfReducers;
	private static int k;
	
	public static int OneIteration(Configuration conf)throws Exception
	{
		conf.set("outputBase",outputPath);
		double e = conf.getDouble("epsilon",Double.MAX_VALUE);//used for judgement of convergence
		/*it should contain the index of nominal features, the value of gamma, 
		 * the value of epsilon (used for judgment of convergence), 
		 * the value of mapreduce.job.reduces to set the number of reducers
		 * */ 
		conf.setInt("currentIteration",currentIteration);
		double gamma = conf.getDouble("gamma", 1.0);
		if(currentIteration > 1)
		{
			//If the algorithm has converged, then stop iterations
			if(Prototypes.convergeJudge(e, conf, outputPath, currentIteration, gamma))
				return 2;
		}
		Job job = Job.getInstance(conf,"kprototypes");
		job.setJarByClass(KprototypesDriver.class);
		job.setNumReduceTasks(numberOfReducers);
		job.setMapperClass(KprototypesMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DataTuple.class);
		job.setReducerClass(KprototypesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath + "/tmp/output" + currentIteration));
		
		MultipleOutputs.addNamedOutput(job,"prototypes", TextOutputFormat.class, Text.class, Text.class);
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args)
	throws Exception
	{
		Configuration conf = new Configuration();
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] args2 = optionParser.getRemainingArgs();
		inputPath = new URI(args2[0]).normalize().toString();
		outputPath = new URI(args2[1]).normalize().toString();
		k = Integer.parseInt(args2[2]);
		maxIteration = Integer.parseInt(args2[3]);
		numberOfReducers = conf.getInt("mapreduce.job.reduces", 1);
		DataTuple.setNotUsed(conf.getStrings("notUsed"));
		conf.setInt("maxIteration",maxIteration);
		conf.setInt("k", k);
		int status;
		for(currentIteration = 0; currentIteration <= maxIteration; ++currentIteration)
		{
			status = OneIteration(conf);
			if(status == 2)
			{
				System.out.println("The K-prototypes converges at the " +currentIteration + " iteration.");
				break;
			}
			else if(status == -1)
			{
				System.out.println("cluster initialize error.");
				break;
			}
		}
		System.out.println("finish");
		postProcess(conf);
	}
	
	public static void postProcess(Configuration conf)
	throws IOException, InterruptedException
	{
		FileSystem hdfs = new Path(outputPath).getFileSystem(conf);
		
		//generate the final k prototypes 
		Path file = new Path(outputPath + "/prototypes.csv");
		if ( hdfs.exists( file )) { hdfs.delete( file, true ); } 
		OutputStream os = hdfs.create(file);
		BufferedWriter br = new BufferedWriter( new OutputStreamWriter(os) );
		ArrayList<DataTuple> prototypes = Prototypes.getPrototypes(conf, maxIteration - 1);
		for(int i = 0; i < prototypes.size();++i)
			br.write(i+", "+prototypes.get(i).toString() + "\n");
		br.close();
		
		//combine the final results
		String concatPath = "";
		for(int i = 0; i < numberOfReducers;++i)
		{
			hdfs.rename(new Path(outputPath + "/tmp/output" + maxIteration + "/part-r-" + String.format("%05d", i)), new Path(outputPath + "/results" + i + ".csv"));
			concatPath += outputPath + "/results" + i + ".csv ";
		}
		Runtime rt = Runtime.getRuntime();
		Process pr = rt.exec("hdfs dfs -getmerge " + concatPath + " results.csv");
		pr.waitFor();
//		System.out.println(pr.getErrorStream());
//		System.out.println(pr.getOutputStream());
		
		//delete all the unnecessary outputs
		hdfs.delete(new Path(outputPath + "/tmp"), true);
		hdfs.close();
	}
}
