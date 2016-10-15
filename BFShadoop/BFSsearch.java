package BFShadoop;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;


public class BFSsearch extends BaseJob{

	public static enum UpdateCounter{
		Update
	}
	
	public static class BFSsearchMapper extends BFSmapper{
		
		public void map(Object key, Text value,Context context)	throws IOException,InterruptedException{
		Node KeyNode = new Node(value.toString());
		super.map(key, value, context, KeyNode);
		}

	}

	
	public static class BFSsearchReducer extends BFSreducer{
		
		public void reduce(Text key, Iterable<Text> value, Context context) 
			throws IOException, InterruptedException{
			Node Out = new Node();
			
			Out = super.reduce(key, value, context, Out);
			
			if(Out.getcolor() == Node.Color.Gray){
				context.getCounter(UpdateCounter.Update).increment(1L);
			}
		}	
	}
	
	private Job getJobConf(String [] args) throws Exception{
		
		JobInfo jobinfo = new JobInfo(){
		@Override
		public Class<? extends Reducer> getCombinerClass(){
			return null;
		}
		
		@Override 
		public Class<? extends Mapper> getMapperClass(){
			return  BFSsearchMapper.class;
		}
		
		@Override
		public Class<?>getJarByClass(){
			return BFSsearch.class;
		}
		
		@Override
		public Class<? extends Reducer> getReducerClass(){
			return BFSsearchReducer.class;
		}
		
		@Override
		public Class<?> getOutputKeyClass() {
			return Text.class;
		}

		@Override
		public Class<?> getOutputValueClass() {
			return Text.class;
		}
	};
		return SetUp("BFSjob",jobinfo);
	}
	
	public int run(String [] args) throws Exception{
		int iter = 0;
		Job job;
		long terminate = 1;
		 	
		while(terminate > 0){
			job = getJobConf(args);
			
			String input, output;
			
			if(iter == 0){
				input = args[0];
			}
			else{
				input = args[1] + iter;
			}
			output = args[1] + (iter + 1);
			
			FileInputFormat.setInputPaths(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			
			job.waitForCompletion(true);
			
			Counters jobcounter = job.getCounters();
			terminate = jobcounter.findCounter(UpdateCounter.Update).getValue();
			
			iter ++;
			
		}
		return iter;
	}
	/*
	public static void main(String [] args) throws Exception{
		int res = ToolRunner.run(new Configuration(), new BFSsearch(),args);
		System.out.println("total Number of iterations: " + res);
	}*/
	public static int runner(String[] pths) throws Exception{
		int iter = ToolRunner.run(new Configuration(), new BFSsearch(),pths);
		return iter;
	}
}
