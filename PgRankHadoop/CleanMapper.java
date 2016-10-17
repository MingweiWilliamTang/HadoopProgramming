package PgRankHadoop;
import java.io.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapred.*;

public class CleanMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>{
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter)
	throws IOException{
		String Svalue = value.toString();
		String[] s = Svalue.split("\t");
		output.collect(new Text(s[0]), new Text(s[1]));
	}

}
