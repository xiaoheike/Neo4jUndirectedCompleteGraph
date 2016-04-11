package nd.esp.com.Neo4jFirstTest;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import nd.esp.com.neo4j.neo4jabstract.AbstractGraph;
import nd.esp.com.neo4j.neo4jabstract.UndirectedGraph;

import org.junit.Test;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerformanceTest {
    public static Logger logger = LoggerFactory.getLogger(PerformanceTest.class);

    public static void main(String[] args) {
        // logger.error("生成10万行数据，每行10个数据，总共100万个数据。这100万个数据由数字0-100万标识。记录插入这些数据花费的时间和磁盘空间使用情况。");
        // List<String[]> tagListArr = new ArrayList<String[]>();
        // int count = 0;
        // long listBuildBegin = System.currentTimeMillis();
        // int insertNum = 100;
        // int everyRowNum = 10;
        // for (int i = 0; i < insertNum; i++) {
        // String[] arr = new String[everyRowNum];
        // for (int j = 0; j < everyRowNum; j++) {
        // arr[j] = Integer.valueOf(count).toString();
        // ++count;
        // }
        // tagListArr.add(arr);
        // }
        // long listBuildEnd = System.currentTimeMillis();
        // logger.error("创建10万行，每行10个数据花费时间{}", listBuildEnd - listBuildBegin);
        // for (String[] strings : tagListArr) {
        // for (int i = 0; i < strings.length; i++) {
        // System.out.println(strings[i]);
        // }
        // }
        // long graphBuildBeginTime = System.currentTimeMillis();
        // Graph graph = new UndirectedGraph(new File("D:\\neo4j\\performan"));
        // graph.buildUndirectedCompleteGraphWithManyRows(tagListArr, "Tag", RelTypes.KNOWS);
        // long graphBuildEndTime = System.currentTimeMillis();
        // logger.error("Neo4j插入{}行，每行{}个数据，一共创建node:{}个,创建关系:{}条,总花费时间{},", insertNum, everyRowNum, insertNum * everyRowNum, insertNum * (everyRowNum
        // * (everyRowNum - 1) / 2), graphBuildEndTime - graphBuildBeginTime);
    }

    @Test
    public void onlyCreateNodes() throws Exception {
        FileUtils.deleteRecursively(new File("D:\\neo4j\\performanOnlyNode"));
        List<String[]> tagListArr = new ArrayList<String[]>();
        int count = 0;
        long listBuildBegin = System.currentTimeMillis();
        int insertNum = 100000;
        int everyRowNum = 10;
        logger.error("只创建节点，数量为:{}", insertNum * everyRowNum);
        for (int i = 0; i < insertNum; i++) {
            String[] arr = new String[everyRowNum];
            for (int j = 0; j < everyRowNum; j++) {
                arr[j] = Integer.valueOf(count).toString();
                ++count;
            }
            tagListArr.add(arr);
        }
        long listBuildEnd = System.currentTimeMillis();
        logger.error("创建{}行，每行{}个数据花费时间{}", insertNum, everyRowNum, listBuildEnd - listBuildBegin);
        long graphBuildBeginTime = System.currentTimeMillis();
        AbstractGraph graph = new UndirectedGraph(new File("D:\\neo4j\\performanOnlyNode"));
        GraphDatabaseService graphDb = graph.getGraphDb();
        Label label = DynamicLabel.label("Tag");
        for (int k = 0; k < tagListArr.size(); k++) {
            Transaction tx = graphDb.beginTx();
            try {
                String[] tagStringArr = tagListArr.get(k);
                for (int i = 0; i < tagStringArr.length; i++) {
                    Node node1 = graphDb.createNode(label);
                    node1.setProperty("name", tagStringArr[i]);
                    for (int j = i + 1; j < tagStringArr.length; j++) {
                        // Map<String, Object> pro = new HashMap<String, Object>();
                        Node node2 = graphDb.createNode(label);
                        node2.setProperty("name", tagStringArr[j]);
                    }

                }
                tx.success();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                tx.close();
            }
        }
        long graphBuildEndTime = System.currentTimeMillis();
        logger.error("只创建节点，数量为:{},消耗时间:{}", insertNum * everyRowNum, graphBuildEndTime - graphBuildBeginTime);
        logger.error("{}", "======================================================================");
    }

    @Test
    public void createNodesAndRelations() throws Exception {
        FileUtils.deleteRecursively(new File("D:\\neo4j\\performanNodesAndRelations"));
        List<String[]> tagListArr = new ArrayList<String[]>();
        int count = 0;
        long listBuildBegin = System.currentTimeMillis();
        int insertNum = 100000;
        int everyRowNum = 10;
        logger.error("创建节点，数量为:{}, 创建联系，数量为:{}", insertNum * everyRowNum, insertNum * (everyRowNum - 1) * (everyRowNum) / 2);
        for (int i = 0; i < insertNum; i++) {
            String[] arr = new String[everyRowNum];
            for (int j = 0; j < everyRowNum; j++) {
                arr[j] = Integer.valueOf(count).toString();
                ++count;
            }
            tagListArr.add(arr);
        }
        long listBuildEnd = System.currentTimeMillis();
        logger.error("创建{}行，每行{}个数据花费时间{}", insertNum, everyRowNum, listBuildEnd - listBuildBegin);
        long graphBuildBeginTime = System.currentTimeMillis();
        AbstractGraph graph = new UndirectedGraph(new File("D:\\neo4j\\performanNodesAndRelations"));
        GraphDatabaseService graphDb = graph.getGraphDb();
        Label label = DynamicLabel.label("Tag");
        RelationshipType knows = DynamicRelationshipType.withName("KNOWS");
        for (int k = 0; k < tagListArr.size(); k++) {
            Transaction tx = graphDb.beginTx();
            try {
                String[] tagStringArr = tagListArr.get(k);
                for (int i = 0; i < tagStringArr.length; i++) {
                    Node node1 = graphDb.createNode(label);
                    node1.setProperty("name", tagStringArr[i]);
                    for (int j = i + 1; j < tagStringArr.length; j++) {
                        Node node2 = graphDb.createNode(label);
                        node2.setProperty("name", tagStringArr[j]);
                        node1.createRelationshipTo(node2, knows);
                    }
                }
                tx.success();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                tx.close();
            }
        }
        long graphBuildEndTime = System.currentTimeMillis();
        logger.error("创建节点，数量为:{}, 创建联系，数量为:{},消耗时间:{}", insertNum * everyRowNum, insertNum * (everyRowNum - 1) * (everyRowNum) / 2, graphBuildEndTime - graphBuildBeginTime);
        logger.error("{}", "======================================================================");
    }

    @Test
    public void createNodeByBatchInsertion() {

    }
}
