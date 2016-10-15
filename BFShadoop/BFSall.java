package BFShadoop;
import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;



public class BFSall {
	  
	private void runClean(String inputPath,String outputPath)
			    throws IOException{
			    	
			    	JobConf conf = new JobConf(BFSall.class);
			    	
			    	conf.setJobName("clean data");
			    	
			    	conf.setOutputKeyClass(Text.class);
			    	conf.setOutputValueClass(Text.class);

			        conf.setInputFormat(TextInputFormat.class);
			        conf.setOutputFormat(TextOutputFormat.class);
			        
			    	FileInputFormat.setInputPaths(conf, new Path(inputPath));
			    	FileOutputFormat.setOutputPath(conf, new Path(outputPath));
			    	
			    	conf.setMapperClass(CleanMapper.class);
			    	conf.setReducerClass(CleanReducer.class);
			    	JobClient.runJob(conf);
			    }
	
	
	public static void main(String args[])throws Exception{
		long start0 = System.nanoTime();
		BFSall get = new BFSall();
		get.runClean(args[0], args[1]);
		long start1 = System.nanoTime(); 
		double t0 = (start1 - start0) / 1000000000.0;
		String[] put = new String[] {args[1],args[2]};
		int a = BFSsearch.runner(put);
		long start2 = System.nanoTime();
		double t1 = (start2 - start1) / 1000000000.0;
		System.out.println(a + " iterations");
		System.out.println( "processing time " + t0);
		System.out.println("total iteration time: " + t1);
	}

}
