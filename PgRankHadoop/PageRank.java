package PgRankHadoop;
import java.io.*;
import java.text.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.util.*;

public class PageRank {
    private static NumberFormat nf = new DecimalFormat("00");
    
    private void runClean(String inputPath,String outputPath)
    throws IOException{
    	
    	JobConf conf = new JobConf(PageRank.class);
    	
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
    
    private void runRankCalculation(String inputPath, String outputPath,int iter) throws IOException {
        JobConf conf = new JobConf(PageRank.class);
	conf.setJobName("Reduce Iteration: " + iter);

	conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
 
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
 
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
 
        conf.setMapperClass(PgRankMapper.class);
	//        conf.setCombinerClass(PgRankReducer.class);
        conf.setReducerClass(PgRankReducer.class);

        JobClient.runJob(conf);
    }
    private void runRankOrdering(String inputPath, String outputPath) throws IOException {
        JobConf conf = new JobConf(RankOrderMapper.class);
 
        conf.setOutputKeyClass(FloatWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
 
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
 
        conf.setMapperClass(RankOrderMapper.class);
 
        JobClient.runJob(conf);
    }
    
    public static String getvalue(String sq,HashMap <String,HashSet<String>> map){
		StringBuilder sb = new StringBuilder();
	    int count = 0;
	    for(String k : map.get(sq)){
	    sb.append(k+",");
	    count ++;
	    }
	    String a = (1.0/75879 + "\t" + count+ "\t" +sb.substring(0,sb.length()-1));
	    return(a);
	}

    public static void main(String[] args) throws Exception {
    	// cleaning the data
    /*
    	Path pt = new Path(args[0]);
    	 FileSystem fs = FileSystem.get(new Configuration());
         BufferedReader sr=new BufferedReader(new InputStreamReader(fs.open(pt)));

    	
  //  	Scanner sc = new Scanner(new BufferedReader(new FileReader(args[0])));
		
    	HashMap <String,HashSet<String>> hmap = new HashMap<String,HashSet<String> >();
    	
    	
    	String cleaned =  args[1] + nf.format(0) + "raw.txt";
		FileWriter fstream = new FileWriter(cleaned);
		BufferedWriter outputfile = new BufferedWriter(fstream);
		Path pt2=new Path("hdfs:/jp./jeka.com:9000/user/jfor/out/abc");
        FileSystem fs2 = FileSystem.get(new Configuration());
        BufferedWriter br2=new BufferedWriter(new OutputStreamWriter(fs2.create(pt2,true)));
                                   // TO append data to a file, use fs.append(Path f)
  
		
		
		
		
		String lines = sr.readLine();
		while(lines != null){

			String[] words = lines.split("\t");
			String s1 = words[0];
			String s2 = words[1];
			if(!hmap.containsKey(s1)){
				HashSet<String> out = new HashSet<String>();
				out.add(s2);
				hmap.put(s1,out);
			}
			else{
				hmap.get(s1).add(s2);
			}
			lines = sr.readLine();
		}
	    
	//   sc.close();
	//    System.out.println("read through " + i + " lines");
		
	    for(String key : hmap.keySet()){
	    	br2.write(key + "\t" + getvalue(key,hmap) + '\n');
	    }
	    br2.close();

*/
    	  	
    	
        PageRank pgrk = new PageRank();
        // clean the data
        pgrk.runClean(args[0], args[0] + "00");
        int runs = 0;
        long [] count_time = new long[5];
        for (; runs < 5; runs++) {
        	long start_time = System.nanoTime();
        	 String input =  args[0] + nf.format(runs);
     	    String output =  args[0] + nf.format(runs + 1);
            //Job 2: Calculate new rank
	    pgrk.runRankCalculation(input, output, runs);
	    count_time[runs] = System.nanoTime() - start_time;
        }
        double s = 0;
        for(int i = 0;i<5;i++){
        	System.out.println(i+1 + "th iteration takes " + (double)count_time[i] / 1000000000.0);
        	s+= (double)count_time[i] / 1000000000.0;
        }
        System.out.println("total time used: " + s);
   /*    
        String finalin = args[0] + nf.format(5);
        String finalout = args[0] + "final";
        pgrk.runRankOrdering(finalin, finalout);
       
        String input = args[0];
        String output = args[1];
        */
   
    }
}