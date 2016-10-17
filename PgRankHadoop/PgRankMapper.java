package PgRankHadoop;
import java.io.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapred.*;

public class PgRankMapper extends MapReduceBase implements Mapper <LongWritable,Text,Text,Text>{
	   
      @Override
     public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
       /*   int PgRankIndex = value.find("|");
          int Total_link_Index = value.find("|", PgRankIndex+1);
          float PgRank = Float.valueOf(Text.decode(value.getBytes(), 0, PgRankIndex));
          int Total_link = Integer.valueOf(Text.decode(value.getBytes(), PgRankIndex+1, Total_link_Index-PgRankIndex));
          
          // Skip pages with no links.
          if(Total_link_Index == -1) return;
   
          String links = Text.decode(value.getBytes(), Total_link_Index+1, value.getLength()-(Total_link_Index+1));
          String[] allOtherPages = links.split(",");
   
          for (String otherPage : allOtherPages){
              float PgRank_cont = PgRank / Total_link;
              output.collect(new IntWritable(Integer.parseInt(otherPage)), new Text(Float.toString(PgRank_cont)));
          }
   
          // Put the original links of the page for the reduce output
          output.collect(key, new Text("|"+Total_link_Index+"|"+links));
          */
    	  String Svalue = value.toString();
    	  
    	  int pgIndex = Svalue.indexOf("\t");
    	  
    	  int pgRankIndex = Svalue.indexOf("\t", pgIndex + 1);
    	 
    	  String page = Svalue.substring(0,pgIndex);
    	  
    	  if(pgRankIndex == -1) return;
    	  
    	  int Total_link_Index = Svalue.indexOf("\t", pgRankIndex + 1);
    	  
    	  float PgRank = Float.valueOf(Svalue.substring(pgIndex + 1, pgRankIndex));
    	  
    	  int Total_link = Integer.valueOf(Svalue.substring(pgRankIndex+1, Total_link_Index));
    	  
    	  String links = Svalue.substring(Total_link_Index + 1, Svalue.length());
    	  String[] allOtherPages = links.split(",");
    	  
    	  for (String otherPage : allOtherPages){
              float PgRank_cont = PgRank / Total_link;
              output.collect(new Text(otherPage), new Text(Float.toString(PgRank_cont)));
          }
   
          // Put the original links of the page for the reduce output
          output.collect(new Text(page), new Text("|" + Total_link+ "\t" + links));
      }
}
