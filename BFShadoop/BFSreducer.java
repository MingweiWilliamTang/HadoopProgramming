package BFShadoop;

import java.io.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.io.*;
public class BFSreducer extends Reducer<Text,Text,Text,Text>{
	
	public Node reduce(Text key,Iterable<Text> value,Context context, Node Out) throws IOException, InterruptedException {
		
		Out.setid(key.toString());
		
		for(Text v : value){
			Node nd = new Node(key.toString() + "\t" + v.toString());
			
			if(nd.getedges().size() > 0){
				// the node contains information about the adjcent edges
				Out.setedges(nd.getedges());
			}
			
			
			if(nd.getdist() < Out.getdist()){
				Out.setdist(nd.getdist());
				Out.setParents(nd.getparent());
			}
			
			if(nd.getcolor().ordinal() > Out.getcolor().ordinal()){
				Out.setcolor(nd.getcolor());
			
			}
		}	
		
		context.write(key, new Text(Out.getNodeString()));
		return Out;
	}
}
