package nd.esp.com.Neo4jFirstTest;

import java.io.File;

import nd.esp.com.neo4j.neo4jabstract.Graph;
import nd.esp.com.neo4j.neo4jabstract.UndirectedGraph;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AddIndexTest {
    Graph graph = null;
    String dir = "D:\\neo4j\\graph";

    @Before
    public void init() {
        this.graph = new UndirectedGraph(new File(dir));
        this.graph.registerShutdownHook();
    }

    @After
    public void destroyTestDatabase() {
        this.graph.shutdown();
    }

    @Test
    public void addConmmentIndex() {
        String label = "Tag";
        String property = "name";
        long timeElapse = 60L;
        this.graph.dropCommentIndex(label);
        this.graph.createIndex(label, property, timeElapse);
        System.out.println("end");
    }

    @Test
    public void addUniqueIndex() {
        String label = "Tag";
        String property = "name";
        this.graph.dropCommentIndex(label);
        this.graph.createUniqueIndex(label, property);
        System.out.println("end");
    }
}
