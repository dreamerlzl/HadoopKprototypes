package mrkprototypes.kprototypes;

import java.util.ArrayList;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DataTuple implements Writable
{
	private String output;
	private ArrayList<String> nominal;// categorical and boolean features
	private static ArrayList<Integer> nominalIndex;
	private static ArrayList<Integer> notUsed;
	private ArrayList<Double> numeric;// ordinal and numeric features
	
	public DataTuple()
	{
		output = "";
		nominal = new ArrayList<String>();
		numeric = new ArrayList<Double>();
	}
	
	public DataTuple(String source)
	{
		output = source;
		nominal = new ArrayList<String>();
		numeric = new ArrayList<Double>();
		String[] splitted = source.split(",");
		int k = 0, j = 0; 
		int sizeOfNominalIndex = nominalIndex.size();
		int sizeOfNotUsed = notUsed.size();
		for(int i = 0; i < splitted.length;++i)
		{
			if(j < sizeOfNotUsed && i == notUsed.get(j))
			{	
				++j;
				continue;
			}
			if(k < sizeOfNominalIndex && i == nominalIndex.get(k))
			{
				++k;
				nominal.add(splitted[i]);
			}
			else 
				numeric.add(Double.parseDouble(splitted[i]));
		}
	}
	
	public ArrayList<Double> getNumeric()
	{
		return numeric;
	}
	
	public ArrayList<String> getNominal()
	{
		return nominal;
	}
	
	public static ArrayList<Integer> getNominalIndex()
	{
		return nominalIndex;
	}
	
	public String getOutput()
	{
		return output;
	}
	
	public static void setNominalIndex(String[] index)
	{
		nominalIndex = new ArrayList<Integer>();
		if(index != null)
			for(int i = 0; i < index.length;++i)
				nominalIndex.add(Integer.parseInt(index[i]));
		else
			nominalIndex.add(-1);//if no specification, then there is no nominal attributes
	}
	
	public static void setNotUsed(String[] index)
	{
		notUsed = new ArrayList<Integer>();
		if(index != null)
			for(int i = 0;i < index.length;++i)
				notUsed.add(Integer.parseInt(index[i]));
		else 
			notUsed.add(-1);
	}
	
	public double metric(DataTuple point, double gamma)
	{
		double numeric_dist = 0.0, nominal_dist = 0.0;
		ArrayList<Double> numeric2 = point.getNumeric();
		ArrayList<String> nomial2 = point.getNominal();
		
		// Euclidean distance for numeric features
		for(int i = 0; i < numeric.size();++i)
		{
			numeric_dist += Math.pow(numeric2.get(i) - numeric.get(i),2);
		}
		// 1 for different nominal values, 0 for same nominal values
		for(int i = 0;i < nominal.size();++i)
		{
			if(nominal.get(i) != nomial2.get(i))
				nominal_dist += 1.0;
		}
		
		return numeric_dist + gamma * nominal_dist;
	}
	
	public String toString()
	{
		return inverse(nominal.toArray(new String[nominal.size()]), numeric.toArray(new Double[numeric.size()]));
	}
	
	//convert the modes and the means back to a DataTuple with same format of training examples
	public static String inverse(String[] nominalAttr, Double[] numericAttr)
	{
		String output = "";
		int nominalLength = nominalAttr.length; 
		int numberOfNotUsed = notUsed.size();
		int numberOfFeatures = nominalLength+ numericAttr.length + numberOfNotUsed;
		int k = 0, j = 0, l = 0;
		for(int i = 0;i < numberOfFeatures;++i)
		{
			if(l < numberOfNotUsed && i == notUsed.get(l))
			{
				output += " "; 
				++l;
			}
			else if(k < nominalLength && i == nominalIndex.get(k))//bug
				output += nominalAttr[k++];
			else 
				output += numericAttr[j++];
			if(i < numberOfFeatures - 1) 
				output += ",";
		}
		return output;
	}

	public void readFields(DataInput in) throws IOException {
		
		int nominalIndexSize = in.readInt();
		nominalIndex = new ArrayList<Integer>();
		for(int i = 0; i < nominalIndexSize;++i)
			nominalIndex.add(in.readInt());
		
		int numericSize = in.readInt();
		numeric = new ArrayList<Double>();
		for(int i = 0; i < numericSize;++i)
			numeric.add(in.readDouble());
		
		int nominalSize = in.readInt();
		in.readLine();
		nominal = new ArrayList<String>();
		for(int i = 0;i < nominalSize;++i)
			nominal.add(in.readLine());
		
		output = in.readLine();
	}

	public void write(DataOutput out) throws IOException {
		
		out.writeInt(nominalIndex.size());
		for(int i = 0; i< nominalIndex.size();++i)
			out.writeInt(nominalIndex.get(i));
		
		out.writeInt(numeric.size());
		for(int i = 0; i < numeric.size();++i)
			out.writeDouble(numeric.get(i));
		
		out.writeInt(nominal.size());
		out.writeBytes("\n");
		for(int i = 0; i < nominal.size();++i)
			out.writeBytes(nominal.get(i) + "\n");
		
		out.writeBytes(output);
	}
}
