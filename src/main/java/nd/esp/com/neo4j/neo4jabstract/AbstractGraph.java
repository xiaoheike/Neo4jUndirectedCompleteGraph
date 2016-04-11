package nd.esp.com.neo4j.neo4jabstract;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.io.fs.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractGraph implements Graph {
    protected GraphDatabaseService graphDb = null;
    protected File file = null;
    Logger logger = LoggerFactory.getLogger(UndirectedGraph.class);

    /**
     * 创建GraphDatabaseService对象。这个方法必须先执行，不然一下的方法会报错。
     * 
     * @param file
     * @return
     * @Title: createGraphDatabaseService
     * @author 821561
     * @date 2016年3月29日 下午7:54:50
     */
    public GraphDatabaseService createGraphDatabaseService(File file) {
        this.file = file;
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(file);
        return graphDb;
    }

    public Transaction beginTranstion() {
        return graphDb.beginTx();
    }

    /**
     * 在数据库中创建Node，建议使用方法:{@link createNodeWithLabel}创建带有label的Node
     * 
     * @return
     * @Title: craeteNode
     * @author 821561
     * @date 2016年3月29日 下午4:04:45
     */
    public Node craeteNode() {
        Transaction tx = this.beginTranstion();
        Node node = null;
        try {
            node = this.graphDb.createNode();
            tx.success();
        } finally {
            tx.close();
        }
        logger.debug("创建一个没有label的node:{}", node);
        return node;
    }

    /**
     * 在数据库中创建带label的Node
     * 
     * @param labelName
     *            Node节点的label名称
     * @return
     * @Title: createNodeWithLabel
     * @author 821561
     * @date 2016年3月29日 下午4:05:54
     */
    public Node createNodeWithLabel(String labelName) {
        Transaction tx = this.beginTranstion();
        Node node = null;
        try {
            node = this.graphDb.createNode(DynamicLabel.label(labelName));
            tx.success();
        } finally {
            tx.close();
        }
        logger.debug("创建一个带有label的node:{}", node);
        return node;
    }

    /**
     * 设置Node属性值
     * 
     * @param node
     * @param properties
     *            Map<key,value> value的值类型必须为如下几种：
     *            boolean or boolean[]
     *            byte or byte[]
     *            short or short[]
     *            int or int[]
     *            long or long[]
     *            float or float[]
     *            double or double[]
     *            char or char[]
     *            java.lang.String or String[]
     * @Title: setNodeProperty
     * @author 821561
     * @date 2016年3月29日 下午4:03:14
     */
    public void setNodeProperty(Node node, Map<String, Object> properties) {
        Transaction tx = this.beginTranstion();
        try {
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                node.setProperty(entry.getKey(), entry.getValue());
            }
            tx.success();
        } finally {
            tx.close();
        }
        logger.debug("设置node:{},属性:{}", node, node.getPropertyKeys());
    }

    /**
     * 在数据库中创建Node并设置该Node属性
     * 
     * @param properties
     *            Map<key,value> value的值类型必须为如下几种：
     *            boolean or boolean[]
     *            byte or byte[]
     *            short or short[]
     *            int or int[]
     *            long or long[]
     *            float or float[]
     *            double or double[]
     *            char or char[]
     *            java.lang.String or String[]
     * @return
     * @Title: createNodeAndSetProperty
     * @author 821561
     * @date 2016年3月29日 下午4:07:32
     */
    public Node createNodeAndSetProperty(Map<String, Object> properties) {
        Transaction tx = this.beginTranstion();
        Node node = null;
        try {
            node = this.graphDb.createNode();
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                node.setProperty(entry.getKey(), entry.getValue());
            }
            tx.success();
        } finally {
            tx.close();
        }
        logger.debug("创建一个没有label的节点:{},并设置属性为:{}", node, node.getPropertyKeys());
        return node;
    }

    /**
     * 在数据库中创建带label的Node并设置该Node属性
     * 
     * @param labelName
     * @param properties
     *            Map<key,value> value的值类型必须为如下几种：
     *            boolean or boolean[]
     *            byte or byte[]
     *            short or short[]
     *            int or int[]
     *            long or long[]
     *            float or float[]
     *            double or double[]
     *            char or char[]
     *            java.lang.String or String[]
     * @return
     * @Title: createNodeWithLabelAndSetProperty
     * @author 821561
     * @date 2016年3月29日 下午4:09:47
     */
    public Node createNodeWithLabelAndSetProperty(String labelName, Map<String, Object> properties) {
        Transaction tx = this.beginTranstion();
        Node node = null;
        try {
            node = this.graphDb.createNode(DynamicLabel.label(labelName));
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                node.setProperty(entry.getKey(), entry.getValue());
            }
            tx.success();
        } finally {
            tx.close();
        }
        logger.debug("创建一个带有label的节点:{},并设置属性值为:{}", node, node.getPropertyKeys());
        return node;
    }

    /**
     * 由于索引创建是异步的，等待索引生效需要时间。建议在没有数据时就创建索引，或者预留足够的时间让索引创建完成。预留的时间可以足够的大，索引创建完成就会自动返回，而不会消耗所有等待的时间在返回。时间单位是秒
     * 
     * @param label
     * @param property
     * @param timeElapse
     *            时间单位是秒，必须足够大
     * @Title: createIndex
     * @author 821561
     * @date 2016年3月29日 下午2:38:38
     */
    public void createIndex(String label, String property, long timeElapse) {
        logger.debug("为label:{} 的属性:{} 创建普通索引", label, property);
        IndexDefinition indexDefinition = null;
        Transaction tx = this.beginTranstion();
        try {
            Schema schema = this.graphDb.schema();
            indexDefinition = schema.indexFor(DynamicLabel.label(label)).on(property).create();
            tx.success();
            // schema.awaitIndexOnline(indexDefinition, timeElapse, TimeUnit.MILLISECONDS);
        } catch (ConstraintViolationException e) {
            logger.error("创建label:{},property:{}索引异常。异常信息内容:{}", label, property, e);
        } finally {
            tx.close();
        }
        this.awaitIndexOnline(indexDefinition, timeElapse);
    }

    private void awaitIndexOnline(IndexDefinition indexDefinition, long timeElapse) {
        Transaction tx = this.beginTranstion();
        try {
            this.graphDb.schema().awaitIndexOnline(indexDefinition, timeElapse, TimeUnit.SECONDS);
        } catch (IllegalStateException e) {
            logger.error("索引创建失败或者等待索引创建完成时间太短。indexDefinition:{},时间:{}异常信息内容:{}", indexDefinition, timeElapse, e);
        } finally {
            tx.close();
        }
    }

    public void dropCommentIndex(String dropIndexLabel) {
        logger.debug("删除label为:{} 的索引", dropIndexLabel);
        Transaction tx = this.beginTranstion();
        try {
            Label label = DynamicLabel.label(dropIndexLabel);
            int count = 0;
            for (IndexDefinition indexDefinition : this.graphDb.schema().getIndexes(label)) {
                logger.warn("{}, {}", this.graphDb.schema().getIndexes(label), count);
                indexDefinition.drop();
                count++;
            }
            tx.success();
        } finally {
            tx.close();
        }
    }

    /**
     * 创建唯一索引。建议使用方法:{@link mergeNodes}
     * 
     * @param label
     * @param property
     * @Title: createUniqueIndex
     * @author 821561
     * @date 2016年3月29日 下午2:56:33
     */
    public void createUniqueIndex(String label, String property) {
        logger.debug("为label:{} 的属性:{} 创建唯一约束索引", label, property);
        Transaction tx = this.beginTranstion();
        try {
            this.graphDb.schema().constraintFor(DynamicLabel.label(label)).assertPropertyIsUnique(property).create();
            tx.success();
        } catch (ConstraintViolationException e) {
            logger.error("创建label:{},property:{}唯一约束异常。异常信息内容:{}", label, property, e);
        } finally {
            tx.close();
        }
    }

    /**
     * CQL关键词MERGE，不存在该node时插入，存在则返回该node。这个方法可以实现唯一node，效果和唯一约束一样
     * 
     * @param label
     * @param parameters
     * @return
     * @Title: mergeNodes
     * @author 821561
     * @date 2016年3月29日 下午6:04:29
     */
    public Node mergeNode(String label, Map<String, Object> parameters) {
        logger.debug("node不存在则创建，存在则返回该node。label为:{}, 属性值为:{}", label, parameters);
        StringBuilder createRelationshipCql = spliceMergeCql(label, parameters);
        Node node = null;
        Transaction tx = this.beginTranstion();
        try {
            Result result = this.graphDb.execute(createRelationshipCql.toString());
            while (result.hasNext()) {
                Map<String, Object> row = result.next();
                for (Entry<String, Object> entry : row.entrySet()) {
                    node = Node.class.cast(entry.getValue());
                }
            }
            // neo4j 官方文档建议:资源完全遍历或者手动关闭资源
            result.close();
            tx.success();
        } catch (QueryExecutionException e) {
            logger.error("创建node执行失败，查询语句:{},参数{}。异常信息内容:{}", label, parameters, e);
        } finally {
            tx.close();
        }
        return node;
    }

    // public List<Node> mergeNodes(String label, List<Map<String, Object>> parametersList) {
    // logger.debug("批量创建node，node不存在则创建，存在则返回该node。label为:{}", label);
    // Transaction tx = this.beginTranstion();
    // try {
    // for (Map<String, Object> parameters : parametersList) {
    // StringBuilder createRelationshipCql = spliceMergeCql(label, parameters);
    // this.graphDb.execute(createRelationshipCql.toString());
    // // neo4j 官方文档建议:资源完全遍历或者手动关闭资源
    // tx.success();
    // }
    // } catch (QueryExecutionException e) {
    // logger.error("创建node执行失败，查询语句:{}。异常信息内容:{}", label, e);
    // } finally {
    // tx.close();
    // }
    // return null;
    // }

    /**
     * 拼接出字符串类似于：MERGE (n:tag{name:1,key:'v'}) RETURN n
     * 
     * @param label
     * @param parameters
     * @return
     * @Title: spliceMergeCql
     * @author 821561
     * @date 2016年3月31日 上午10:46:45
     */
    private StringBuilder spliceMergeCql(String label, Map<String, Object> parameters) {
        StringBuilder createRelationshipCql = new StringBuilder("MERGE (n:");
        createRelationshipCql.append(label);
        createRelationshipCql.append("{");
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            createRelationshipCql.append(entry.getKey());
            createRelationshipCql.append(":");
            if (!(entry.getValue() instanceof String)) {
                createRelationshipCql.append(entry.getValue());
            } else {
                createRelationshipCql.append("'");
                createRelationshipCql.append(entry.getValue());
                createRelationshipCql.append("'");
            }
            createRelationshipCql.append(",");
        }
        // 去除最后添加的逗号
        createRelationshipCql.deleteCharAt(createRelationshipCql.length() - 1);
        createRelationshipCql.append("})");
        createRelationshipCql.append(" RETURN n");
        logger.info("拼接完成后的mergeCQL语句:{}", createRelationshipCql.toString());
        return createRelationshipCql;
    }

    // public Result executeCql(String queryString, Map<String, Object> parameters) {
    // logger.debug("查询语句:{}, 参数:{}", queryString, parameters);
    // Transaction tx = this.beginTranstion();
    // Result result = null;
    // try {
    // if (parameters.isEmpty()) {
    // result = this.graphDb.execute(queryString);
    // } else {
    // result = this.graphDb.execute(queryString, parameters);
    // }
    // } catch (QueryExecutionException e) {
    // logger.error("查询语句:{},参数{},执行失败。异常信息内容:{}", queryString, parameters, e);
    // } finally {
    // tx.close();
    // }
    // return result;
    // }

    public Relationship createRelationshipTo(Node fromNode, Node toNode, RelationshipType relType) {
        Transaction tx = this.beginTranstion();
        Relationship relationship;
        try {
            relationship = fromNode.createRelationshipTo(toNode, relType);
            tx.success();
        } finally {
            tx.close();
        }
        logger.debug("创建node:{}与node:{}的联系", fromNode, toNode, relType);
        return relationship;
    }

    public Map<String, Relationship> createUniqueRelationshipTo(String createUniqueRelationshipStr, Map<String, Object> parameters) {
        logger.debug("联系不存在则创建，存在则返回。创建语句:{},参数:{}", createUniqueRelationshipStr, parameters);
        Transaction tx = this.beginTranstion();
        Map<String, Object> relationshipTemp = null;
        Map<String, Relationship> relationship = new IdentityHashMap<String, Relationship>();
        try {
            Result result = this.graphDb.execute(createUniqueRelationshipStr, parameters);
            while (result.hasNext()) {
                relationshipTemp = result.next();
                for (Map.Entry<String, Object> entry : relationshipTemp.entrySet()) {
                    relationship.put(entry.getKey(), Relationship.class.cast(entry.getValue()));
                }
            }
            // neo4j 官方文档建议要遍历或者手动关闭资源
            result.close();
            tx.success();
        } catch (QueryExecutionException e) {
            logger.error("联系不存在则创建，存在则返回。创建语句:{},参数:{},执行失败。异常信息内容:{}", createUniqueRelationshipStr, parameters, e);
        } finally {
            tx.close();
        }
        return relationship;
    }

    /**
     * @param relationship
     * @param properties
     *            Map<key,value> value的值类型必须为如下几种：
     *            boolean or boolean[]
     *            byte or byte[]
     *            short or short[]
     *            int or int[]
     *            long or long[]
     *            float or float[]
     *            double or double[]
     *            char or char[]
     *            java.lang.String or String[]
     * @Title: setRelationshipProperty
     * @author 821561
     * @date 2016年3月29日 下午6:03:02
     */
    public void setRelationshipProperty(Relationship relationship, Map<String, Object> properties) {
        Transaction tx = this.beginTranstion();
        try {
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                relationship.setProperty(entry.getKey(), entry.getValue());
            }
            tx.success();
        } finally {
            tx.close();
        }
        logger.debug("设置联系:{}的属性值为:{}", relationship, properties);
    }

    /**
     * @param fromNode
     * @param toNode
     * @param relType
     * @param properties
     *            Map<key,value> value的值类型必须为如下几种：
     *            boolean or boolean[]
     *            byte or byte[]
     *            short or short[]
     *            int or int[]
     *            long or long[]
     *            float or float[]
     *            double or double[]
     *            char or char[]
     *            java.lang.String or String[]
     * @return
     * @Title: createRelationshipAndSetProperty
     * @author 821561
     * @date 2016年3月29日 下午6:03:31
     */
    public Relationship createRelationshipAndSetProperty(Node fromNode, Node toNode, RelationshipType relType, Map<String, Object> properties) {
        Transaction tx = this.beginTranstion();
        Relationship relationship;
        try {
            relationship = fromNode.createRelationshipTo(toNode, relType);
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                relationship.setProperty(entry.getKey(), entry.getValue());
            }
            tx.success();
        } finally {
            tx.close();
        }
        logger.debug("设置fromNode:{}与toNode:{}的联系为:{}, 属性值为:{}", fromNode, toNode, relationship, properties);
        return relationship;
    }

    public Map<String, Object> getRelationShipBetweenTwoNode(String queryRelationship, Map<String, Object> nodesParameters) {
        logger.debug("查询两个节点之间的关系语句:{},参数:{}", queryRelationship, nodesParameters);
        Transaction tx = this.beginTranstion();
        Map<String, Object> relationship = new HashMap<String, Object>();
        try {
            Result relationshipTemp = this.graphDb.execute(queryRelationship, nodesParameters);
            if (relationshipTemp.hasNext()) {
                relationship = relationshipTemp.next();
            }
            relationshipTemp.close();
        } catch (QueryExecutionException e) {
            logger.error("查询联系失败。参数:{},异常信息:{}", nodesParameters, e);
        } finally {
            tx.close();
        }
        return relationship;
    }

    /**
     * 调用此方法会删除所有的node和relation
     * 
     * @Title: clearAllDataAndRelation
     * @author 821561
     * @date 2016年3月31日 下午3:03:52
     */
    public void clearAllDataAndRelation() {
        String deleteAllCql = "match(n) detach delete n";
        Transaction tx = this.beginTranstion();
        try {
            this.graphDb.execute(deleteAllCql);
        } catch (Exception e) {
            logger.error("删除所有node和relation错误,语句:{},异常信息:{}", deleteAllCql, e);
        } finally {
            tx.close();
        }
    }

    /**
     * 通过这种方式可以删除所有的node和relation
     * 
     * @Title: deleteDirRecursively
     * @author 821561
     * @date 2016年4月1日 下午3:51:33
     */
    public void deleteDirRecursively() {
        logger.info("删除所有的node和relation");
        try {
            FileUtils.deleteRecursively(file);
        } catch (IOException e) {
            logger.error("删除文件夹出错，异常信息:{}", e);
        }
    }

    public Object getRelationshipProperty(Relationship relationship, String key) {
        logger.debug("获得联系:{},属性为:{}的值", relationship, key);
        Transaction tx = this.beginTranstion();
        Object property = null;
        try {
            property = relationship.getProperty(key);
        } catch (NullPointerException e) {
            logger.error("传入的属性:{},在联系:{}中不存在", key, relationship);
        } finally {
            tx.close();
        }
        return property;
    }

    public Map<String, Object> getRelationshipProperties(Relationship relationship, String... keys) {
        logger.debug("获得联系:{},属性为:{}的值", relationship, keys);
        Transaction tx = this.beginTranstion();
        Map<String, Object> properties = null;
        try {
            properties = relationship.getProperties(keys);
        } catch (NullPointerException e) {
            logger.error("传入的属性:{},一个或者多个在联系:{}中不存在", keys, relationship);
        } finally {
            tx.close();
        }
        return properties;
    }

    public Object getNodeProperty(Node node, String key) {
        logger.debug("获得联系:{},属性为:{}的值", node, key);
        Transaction tx = this.beginTranstion();
        Object property = null;
        try {
            property = node.getProperty(key);
        } catch (NullPointerException e) {
            logger.error("传入的属性:{},在联系:{}中不存在", key, node);
        } finally {
            tx.close();
        }
        return property;
    }

    public Map<String, Object> getNodeProperties(Node node, String... keys) {
        logger.debug("获得联系:{},属性为:{}的值", node, keys);
        Transaction tx = this.beginTranstion();
        Map<String, Object> properties = null;
        try {
            properties = node.getProperties(keys);
        } catch (NullPointerException e) {
            logger.error("传入的属性:{},一个或者多个在联系:{}中不存在", keys, node);
        } finally {
            tx.close();
        }
        return properties;
    }

    public void shutdown() {
        graphDb.shutdown();
    }

    /**
     * 注册一个关闭的hook，用于保证jvm异常退出是Neo4j能够正常关闭
     * 
     * @Title: registerShutdownHook
     * @author 821561
     * @date 2016年3月29日 下午8:05:44
     */
    public void registerShutdownHook()
    {
        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        });
    }

    public GraphDatabaseService getGraphDb() {
        return graphDb;
    }

    public void setGraphDb(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }
}
