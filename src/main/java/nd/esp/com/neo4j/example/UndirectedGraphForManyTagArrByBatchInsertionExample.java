package nd.esp.com.neo4j.example;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import nd.esp.com.neo4j.model.RelTypes;
import nd.esp.com.neo4j.neo4jabstract.InitialGraph;

public class UndirectedGraphForManyTagArrByBatchInsertionExample {

    public static void main(String[] args) throws IOException {
        String[] tagArr1 = new String[] { "A", "B", "C", "D", "K", "M" };
        String[] tagArr3 = new String[] { "D", "O", "P", "Q" };
        String[] tagArr4 = new String[] { "Q", "1", "2", "3", "4" };
        String[] tagArr5 = new String[] { "1", "2", "5", "6" };
        List<String[]> tagList = new ArrayList<String[]>();
        tagList.add(tagArr1);
        tagList.add(tagArr3);
        tagList.add(tagArr4);
        tagList.add(tagArr5);
        InitialGraph initialGraph = new InitialGraph(new File("D:/neo4j/test3"));
        initialGraph.buildUndirectedCompleteGraphByBatchInsertionWithManyRows(tagList, "Tag", RelTypes.KNOWS);
        initialGraph.shutdown();
        System.out.println("end");
    }
}
