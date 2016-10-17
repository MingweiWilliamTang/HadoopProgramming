package PgRankHadoop;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class RankOrderMapper extends MapReduceBase implements Mapper<LongWritable,Text,FloatWritable,Text>{
	@Override
	public void map(LongWritable key, Text value, OutputCollector<FloatWritable, Text> output,Reporter args)throws IOException{
	String Svalue = value.toString();
	String []split = Svalue.split("\t");
	float frank = Float.parseFloat(split[1]);
	FloatWritable rank = new FloatWritable(-frank);
	Text page = new Text(split[0]);
	output.collect(rank, page);
	}
		
}
