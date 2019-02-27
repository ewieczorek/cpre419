import java.util.*;

public class GraphNode {
	private String Vertex;
	private List<String> Edges = new ArrayList<String>();
	
	GraphNode(String vertex) {
		this.Vertex = vertex;
		this.Edges = new ArrayList<String>();	
	}
	
	public void AddEdge(String edge) {
		if(!this.Edges.contains(edge) && !edge.equals(Vertex)) {			
			this.Edges.add(edge);
		}
	}
	
	public String getVertex() {
		return this.Vertex;
	}
	
	public List<String> getEdges() {
		return this.Edges;
	}
}
