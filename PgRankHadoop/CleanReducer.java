package PgRankHadoop;
import java.util.*;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
public class CleanReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text>{
	@Override
	public void reduce(Text key, Iterator<Text> value,OutputCollector<Text,Text> output, Reporter reporter)
	throws IOException{
		StringBuilder sb = new StringBuilder();
			int count = 0;
			while(value.hasNext()){
				count ++;
				String s = value.next().toString();
				sb.append(s + "|" + 0 + ",");
			}
			output.collect(key, new Text( 1 +
					"\t" + count + "\t" + sb.substring(0,sb.length()-1)));
			}
/*
		else{
			Random Rd = new Random();
			HashSet<String> allnodes = new HashSet<String>();
			while(value.hasNext()){
				allnodes.add(value.next().toString());
			}
			for(String s:allnodes){
				sb.append(s + "|" + (float)Rd.nextDouble() + ",");
			}
			output.collect(key, new Text(sb.substring(0,sb.length()-1) + "\t" + 0 +
					"\t" + "Gray" + "\t" + "No"));
		}
	
		*/
	}
