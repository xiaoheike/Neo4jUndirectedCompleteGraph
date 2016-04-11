package nd.esp.com.neo4j.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nd.esp.com.neo4j.model.RelTypes;
import nd.esp.com.neo4j.neo4jabstract.UndirectedGraph;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class UndirectedGraphForManyTagArrExample {
    public static void main(String[] args) throws Exception {
        String[] tagArr1 = new String[] { "A", "B", "C", "D", "K", "M" };
        String[] tagArr3 = new String[] { "D", "O", "P", "Q" };
        String[] tagArr4 = new String[] { "Q", "1", "2", "3", "4" };
        String[] tagArr5 = new String[] { "1", "2", "5", "6" };
        List<String[]> tagList = new ArrayList<String[]>();
        tagList.add(tagArr1);
        tagList.add(tagArr3);
        tagList.add(tagArr4);
        tagList.add(tagArr5);
        UndirectedGraph graph = new UndirectedGraph(new File("\\undirectedgraphformanytagarr"));
        graph.buildUndirectedCompleteGraphWithManyRows(tagList, "Tag", RelTypes.KNOWS);

        System.out.println("图数据：");
        int count = 1;
        for (String[] strings : tagList) {
            StringBuilder sb = new StringBuilder("第" + count + "行：");
            for (String str : strings) {
                sb.append(str);
                sb.append(" ");
            }
            ++count;
            System.out.println(sb);
        }
        while (true) {
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            System.out.print("请输入要查找的节点：");
            String tagName = in.readLine();
            String query = "MATCH(n:Tag{name:{tagName}}) return n";
            Map<String, Object> par = new HashMap<String, Object>();
            par.put("tagName", tagName);
            Transaction transaction = graph.beginTranstion();
            try {
                Result execute = graph.getGraphDb().execute(query, par);
                if (execute.hasNext()) {
                    Map<String, Object> nodes = execute.next();
                    for (Map.Entry<String, Object> entry : nodes.entrySet()) {
                        Iterable<Relationship> relationships = Node.class.cast(entry.getValue()).getRelationships(RelTypes.KNOWS, Direction.BOTH);
                        for (Relationship relationship : relationships) {
                            Node startNode = relationship.getStartNode();
                            Node endNode = relationship.getEndNode();
                            Object startNodeProperty = graph.getNodeProperty(startNode, "name");
                            // System.out.println(tagName + "--" + beginNodeProperty);
                            Object endNodeProperty = graph.getNodeProperty(endNode, "name");
                            // System.out.println(tagName + "--" + startNodeProperty);
                            Object relationshipProperty = graph.getRelationshipProperty(relationship, "weight");
                            System.out.println(startNodeProperty + "--" + relationshipProperty + "--" + endNodeProperty);
                        }
                    }
                } else {
                    System.out.println("您输入的节点不存在！");
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                transaction.close();
            }
            // graph.shutdown();
        }
    }
}
