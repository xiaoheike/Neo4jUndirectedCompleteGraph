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
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitialGraph {
    private Logger logger = LoggerFactory.getLogger(InitialGraph.class);
    private BatchInserter inserter = null;

    public InitialGraph(File file) throws IOException {
        inserter = BatchInserters.inserter(file);
    }

    public void buildUndirectedCompleteGraphByBatchInsertionWithManyRows(List<String[]> tagListArr, String label, RelTypes relationship) {
        for (int rowNumber = 0; rowNumber < tagListArr.size(); rowNumber++) {
            String[] tagStringArr = tagListArr.get(rowNumber);
            this.buildUndirectedCompleteGraphByBatchInsertionWithOneRow(tagStringArr, label, relationship);
        }
    }

    public void buildUndirectedCompleteGraphByBatchInsertionWithOneRow(String[] tagStringArr, String label, RelTypes relationship) {
        // BatchInserter inserter = null;
        String relWeight = "weight";
        String tagName = "name";
        for (int i = 0; i < tagStringArr.length; i++) {
            for (int j = i + 1; j < tagStringArr.length; j++) {
                try {
                    Map<String, Object> fromNodeParameters = new HashMap<String, Object>();
                    Map<String, Object> toNodeParameters = new HashMap<String, Object>();
                    fromNodeParameters.put(tagName, tagStringArr[i]);
                    toNodeParameters.put(tagName, tagStringArr[j]);

                    Label tagLabel = DynamicLabel.label(label);
                    // inserter.createDeferredSchemaIndex(tagLabel).on(tagName).create();

                    long FromNode = inserter.createNode(fromNodeParameters, tagLabel);
                    long toNode = inserter.createNode(toNodeParameters, tagLabel);
                    Map<String, Object> properties = new HashMap<String, Object>();
                    properties.put(relWeight, 1L);
                    inserter.createRelationship(FromNode, toNode, relationship, properties);
                } catch (ConstraintViolationException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void shutdown() {
        if (inserter != null)
        {
            inserter.shutdown();
        }
    }

}
