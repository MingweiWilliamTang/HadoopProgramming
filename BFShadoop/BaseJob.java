package BFShadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
public abstract class BaseJob extends Configured implements Tool{
	//set up configuration for map and reduce jobs
	
	
	protected Job SetUp(String JobName, JobInfo jobinfo) throws Exception{
		Job job = new Job(new Configuration(), JobName);
		
		//set up JarClass
		job.setJarByClass(jobinfo.getJarByClass());
		
		//set up Mapper Reducer
		job.setMapperClass(jobinfo.getMapperClass());
		
		if((jobinfo.getCombinerClass() != null)){
			job.setCombinerClass(jobinfo.getCombinerClass());
		}
		
		job.setReducerClass(jobinfo.getReducerClass());
		
	//	job.setNumReduceTasks(3);
		
		//output key and value
		
		job.setOutputKeyClass(jobinfo.getOutputKeyClass());
		job.setOutputValueClass(jobinfo.getOutputValueClass());
		 
		return job;
	}

	
	protected abstract class JobInfo{
		public abstract Class<?> getJarByClass();
		public abstract Class<? extends Mapper> getMapperClass();
		public abstract Class<? extends Reducer> getCombinerClass();
		public abstract Class<? extends Reducer> getReducerClass();
		public abstract Class<?> getOutputKeyClass();
		public abstract Class<?> getOutputValueClass();
	}
}
