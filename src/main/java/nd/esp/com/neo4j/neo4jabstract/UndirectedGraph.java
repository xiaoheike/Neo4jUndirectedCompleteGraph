package nd.esp.com.neo4j.neo4jabstract;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nd.esp.com.neo4j.model.RelTypes;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UndirectedGraph extends AbstractGraph {
    Logger logger = LoggerFactory.getLogger(UndirectedGraph.class);

    public UndirectedGraph(File file) {
        super.createGraphDatabaseService(file);
    }

    /**
     * 多行数据，构建"无向完全图"
     * 
     * @param tagListArr
     * @param relType
     *            TODO
     * @param graph
     * @Title: buildUndirectedCompleteGraphWithManyRows
     * @author 821561
     * @date 2016年3月30日 下午6:55:07
     */
    public void buildUndirectedCompleteGraphWithManyRows(List<String[]> tagListArr, String label, RelTypes relationship) {
        for (int rowNumber = 0; rowNumber < tagListArr.size(); rowNumber++) {
            String[] tagStringArr = tagListArr.get(rowNumber);
            this.buildUndirectedCompleteGraphWithOneRow(tagStringArr, label, relationship);
        }
    }

    public void buildUndirectedCompleteGraphWithOneRow(String[] tagStringArr, String label, RelTypes relationship) {
        String tagName = "name";
        String relationshipProperty = "weight";
        // 我偷懒了，直接写死字符串
        String queryRelationship = "match(fromNode:Tag{name:{fromNodeName}})-[relationship:" + relationship + "]-(toNode:Tag{name:{toNodeName}}) return relationship";
        for (int i = 0; i < tagStringArr.length; i++) {
            for (int j = i + 1; j < tagStringArr.length; j++) {
                Map<String, Object> fromNodeParameters = new HashMap<String, Object>();
                Map<String, Object> toNodeParameters = new HashMap<String, Object>();
                Map<String, Object> relationshipParameters = new HashMap<String, Object>();
                fromNodeParameters.put(tagName, tagStringArr[i]);
                relationshipParameters.put("fromNodeName", tagStringArr[i]);
                toNodeParameters.put(tagName, tagStringArr[j]);
                relationshipParameters.put("toNodeName", tagStringArr[j]);
                Node fromNode = this.mergeNode(label, fromNodeParameters);
                Node toNode = this.mergeNode(label, toNodeParameters);
                Map<String, Object> nodePara = new HashMap<String, Object>();
                nodePara.put("fromNodeName", tagStringArr[i]);
                nodePara.put("toNodeName", tagStringArr[j]);

                Map<String, Object> relationShipBetweenTwoNode = this.getRelationShipBetweenTwoNode(queryRelationship, nodePara);
                Map<String, Object> properties = new HashMap<String, Object>();

                properties.put(relationshipProperty, 1L);
                if (relationShipBetweenTwoNode.isEmpty()) {
                    this.createRelationshipAndSetProperty(fromNode, toNode, RelTypes.KNOWS, properties);
                } else {
                    for (Map.Entry<String, Object> entry : relationShipBetweenTwoNode.entrySet()) {
                        Long weight = (Long) this.getRelationshipProperty(Relationship.class.cast(entry.getValue()), relationshipProperty);
                        Map<String, Object> values = new HashMap<String, Object>();
                        values.put(relationshipProperty, ++weight);
                        this.setRelationshipProperty(Relationship.class.cast(entry.getValue()), values);
                    }
                }
            }
        }
    }

    public void buildUndirectedCompleteGraphByBatchInsertionWithManyRows(List<String[]> tagListArr, String label, RelTypes relationship) {
        for (int rowNumber = 0; rowNumber < tagListArr.size(); rowNumber++) {
            String[] tagStringArr = tagListArr.get(rowNumber);
            this.buildUndirectedCompleteGraphByBatchInsertionWithOneRow(tagStringArr, label, relationship);
        }
    }

    public void buildUndirectedCompleteGraphByBatchInsertionWithOneRow(String[] tagStringArr, String label, RelTypes relationship) {
        BatchInserter inserter = null;
        String relWeight = "weight";
        String tagName = "name";
        for (int i = 0; i < tagStringArr.length; i++) {
            for (int j = i + 1; j < tagStringArr.length; j++) {
                try {
                    Map<String, Object> fromNodeParameters = new HashMap<String, Object>();
                    Map<String, Object> toNodeParameters = new HashMap<String, Object>();
                    fromNodeParameters.put(tagName, tagStringArr[i]);
                    toNodeParameters.put(tagName, tagStringArr[j]);

                    inserter = BatchInserters.inserter(this.getFile());
                    Label tagLabel = DynamicLabel.label(label);
                    inserter.createDeferredSchemaIndex(tagLabel).on(tagName).create();

                    long FromNode = inserter.createNode(fromNodeParameters, tagLabel);
                    long toNode = inserter.createNode(fromNodeParameters, tagLabel);
                    Map<String, Object> properties = new HashMap<String, Object>();
                    properties.put(relWeight, 1L);
                    inserter.createRelationship(FromNode, toNode, relationship, properties);
                } catch (ConstraintViolationException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (inserter != null)
                    {
                        inserter.shutdown();
                    }
                }
            }
        }
    }

    public static void main(String[] args) {}
}
