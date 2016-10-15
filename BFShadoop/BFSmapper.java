package BFShadoop;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;

public class BFSmapper extends Mapper<Object,Text,Text,Text>{

	//@Override
	public void map(Object key, Text value, Context context,Node KeyNode)throws IOException,InterruptedException{
//	Node KeyNode = new Node(value.toString());
		if(KeyNode.getcolor() == Node.Color.Gray){
			// turns all its adjacent node to gray and change their distance
			
			for(String adjId:KeyNode.getedges()){
			Node adj = new Node();
			
			adj.setid(adjId);
			
			adj.setdist(KeyNode.getdist() + 1);
			
			adj.setcolor(Node.Color.Gray);
			
			adj.setParents(KeyNode.getid());
			
			context.write(new Text(adj.getid()), adj.getNodeString());
			}
				KeyNode.setcolor(Node.Color.Black);
			
		}
		context.write(new Text(KeyNode.getid()), KeyNode.getNodeString());
	}
}
