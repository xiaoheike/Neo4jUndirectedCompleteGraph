package nd.esp.com.neo4j.model;

import org.neo4j.graphdb.RelationshipType;

public class T implements RelationshipType {
    Long weight;

    public T(Long weight) {
        this.weight = weight;
    }

    public String name() {
        return "L";
    }

    public Long getWeight() {
        return weight;
    }

    public void setWeight(Long weight) {
        this.weight = weight;
    }
}
