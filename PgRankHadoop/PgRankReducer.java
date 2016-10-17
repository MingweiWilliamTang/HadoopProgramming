package PgRankHadoop;
import java.util.*;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PgRankReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text>{
	 private static final float damping = 0.85F;
//	 private static final int N = 1632803;
	    @Override
	    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
	        String split = "";
	        float sumShareOtherPageRanks = 0;
	        String links = "";
	        String pageWithRank;
	 
	        // For each otherPage:
	        // - check control characters
	        // - calculate pageRank share <rank> / count(<links>)
	        // - add the share to sumShareOtherPageRanks
	        while(values.hasNext()){
	            pageWithRank = values.next().toString();
	 
	            if(pageWithRank.startsWith("|")){
	                links = "\t" + pageWithRank.substring(1);
	                continue;
	            }
	 
	            split = pageWithRank;
	            float pageRank_cont = Float.valueOf(split);
	   //         int countOutLinks = Integer.valueOf(split[1]);
	 
	            sumShareOtherPageRanks += pageRank_cont;
	        }
	 
	        float newRank = damping * sumShareOtherPageRanks+ (1-damping);
	 
	        out.collect(key, new Text(newRank + links));
	    }
	}
