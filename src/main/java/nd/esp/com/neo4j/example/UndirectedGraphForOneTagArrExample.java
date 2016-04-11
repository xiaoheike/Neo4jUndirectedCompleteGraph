package nd.esp.com.neo4j.example;

import java.io.File;

import nd.esp.com.neo4j.model.RelTypes;
import nd.esp.com.neo4j.neo4jabstract.UndirectedGraph;

public class UndirectedGraphForOneTagArrExample {
    public static void main(String[] args) {
        String[] strArr = new String[] { "A", "B", "C", "D", "E", "F" };
        // List<String[]> tagList = new ArrayList<String[]>();
        // tagList.add(strArr);
        UndirectedGraph graph = new UndirectedGraph(new File("D:\\neo4j\\test1"));
        graph.buildUndirectedCompleteGraphWithOneRow(strArr, "Tag", RelTypes.KNOWS);
        System.out.println("end");
    }
}
