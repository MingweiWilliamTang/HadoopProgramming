package BFShadoop;
import org.apache.hadoop.io.*;
import java.util.ArrayList;



public class Node {
	public static enum Color{
		White,Gray,Black
	}
	private String id;
	private ArrayList<String> edges = new ArrayList<String>();
	private float dist;
	private String Parent;
	private Color color = Color.White;
	
	// constructor
	public Node(){
		this.dist = Float.MAX_VALUE;
		this.edges = new ArrayList<String>();
		this.color = Color.White;
		this.Parent = null;
	}
	
	public Node(String NodeString){
		String[] splits = NodeString.split("\t");
		// the id of the String
		this.id = splits[0];
		
		// all the nodes the current node links
		this.edges = new ArrayList<String>();
		
		if(splits[1].length() > 0){
		
		String [] neighbors =splits[1].split(",");
	//	int NumAdj = neighbors.length;
		//for(int i = 0; i< NumAdj; i++){
			for(String s:neighbors){
			this.edges.add(s);
		}
		}
		

		// the distance from source node
		if(splits[2].equals(Float.toString(Float.MAX_VALUE))){
			this.dist = Float.MAX_VALUE;
		}
		else{
			this.dist = Float.parseFloat(splits[2]);
		}
		
		// set up the color of the node
		this.color = Color.valueOf(splits[3]);
		
		// set up the parent
		this.Parent = splits[4];
		
	}
	
	
	// the get function that used to query the fields in the node class
	public String getid(){
		return this.id;
	}
	
	public float getdist(){
		return this.dist;
	}
	
	public ArrayList<String> getedges(){
		return this.edges;
	}
	
	public String getparent(){
		return this.Parent;
	}
	
	public Color getcolor(){
		return this.color;
	}
	
	
	// set function
	public void setedges(ArrayList<String> E){
		this.edges = E;
	}
	
	public void setid(String id){
		this.id = id;
	}
	
	public void setdist(float dist){
		this.dist = dist;
	}
	
	public void setParents(String parent){
		this.Parent = parent;
	}
	public void setcolor(Color color){
		this.color = color;
	}
	 
	public Text getNodeString(){
		
		// deal with adjacent edges first
		StringBuilder sb = new StringBuilder();
		
		int Numedges = this.edges.size();
		
		if(Numedges > 0){
			
		for(int i = 0;i < Numedges - 1;i++){
			String E = this.edges.get(i);
			sb.append(E).append(",");
		}
	
		String E = this.edges.get(Numedges - 1);
		
		sb.append(E);
		}
		
		sb.append("\t");
		// add distance distance to string
		if(this.dist < Float.MAX_VALUE){
			sb.append(this.dist);
		}
		else{
			sb.append(Float.MAX_VALUE);
		}
		
		sb.append("\t");
		
		// color
		sb.append(this.color.toString());
		
		sb.append("\t");
		// parent
		sb.append(this.Parent);
		
		return new Text(sb.toString());
	}
	
}
