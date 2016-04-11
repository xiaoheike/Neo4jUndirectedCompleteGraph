package nd.esp.com.neo4j.neo4jabstract;

import java.io.File;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public interface Graph {
    public GraphDatabaseService createGraphDatabaseService(File file);

    public Transaction beginTranstion();

    public Node craeteNode();

    public Node createNodeWithLabel(String labelName);

    public void setNodeProperty(Node node, Map<String, Object> properties);

    public Node createNodeAndSetProperty(Map<String, Object> properties);

    public Node createNodeWithLabelAndSetProperty(String labelName, Map<String, Object> properties);

    public void createIndex(String label, String property, long timeElapse);

    public void createUniqueIndex(String label, String property);

    public Node mergeNode(String label, Map<String, Object> parameters);

    // public Result executeCql(String queryString, Map<String, Object> parameters);

    public Relationship createRelationshipTo(Node fromNode, Node toNode, RelationshipType relType);

    public void setRelationshipProperty(Relationship relationship, Map<String, Object> properties);

    public Relationship createRelationshipAndSetProperty(Node fromNode, Node toNode, RelationshipType relType, Map<String, Object> properties);

    public void registerShutdownHook();

    public void shutdown();

    public void dropCommentIndex(String label);

    public Map<String, Relationship> createUniqueRelationshipTo(String createUniqueRelationshipStr, Map<String, Object> parameters);

    public Map<String, Object> getRelationShipBetweenTwoNode(String queryRelationship, Map<String, Object> nodesParameters);

    public Object getRelationshipProperty(Relationship relationship, String key);

    public Map<String, Object> getRelationshipProperties(Relationship relationship, String... keys);

    public void clearAllDataAndRelation();

    public Object getNodeProperty(Node node, String key);

    public Map<String, Object> getNodeProperties(Node node, String... keys);

    // public void buildUndirectedCompleteGraphWithOneRow(String[] tagStringArr, String label, RelTypes relationship);

    // public void buildUndirectedCompleteGraphWithManyRows(List<String[]> tagListArr, String label, RelTypes relationship);
}
