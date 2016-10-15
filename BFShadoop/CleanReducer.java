package BFShadoop;

import java.util.*;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
public class CleanReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text>{
	@Override
	public void reduce(Text key, Iterator<Text> value,OutputCollector<Text,Text> output, Reporter reporter)
	throws IOException{
		StringBuilder sb = new StringBuilder();
			while(value.hasNext()){
				String s = value.next().toString();
				sb.append(s + ",");
			}
			if(key.toString().equals("18")){
			output.collect(key, new Text(sb.substring(0,sb.length()-1) + "\t" + 0 +
					"\t" + "Gray" + "\t" + "No"));
			}
			else{
				output.collect(key, new Text(sb.substring(0,sb.length()-1) + "\t" + Float.MAX_VALUE +
						"\t" + "White" + "\t" + "No"));
			}
	}
}
