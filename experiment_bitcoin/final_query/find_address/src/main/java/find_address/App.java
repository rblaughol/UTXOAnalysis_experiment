package find_address;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
//import org.janusgraph.core.*;
//import org.janusgraph.core.schema.JanusGraphManagement;
//import java.io.File;
//import java.io.IOException;
//import java.util.List;
//import org.apache.commons.io.FileUtils;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
//import org.janusgraph.core.JanusGraphManagement;
import org.janusgraph.core.JanusGraphTransaction;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
//import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class App {
  public static void main(String[] args) {
    JanusGraph graph = JanusGraphFactory.open("/public/home/blockchain/master/experiment/final_query/janusgraph-hbase.properties");
    System.out.println("-----module-----");
    GraphTraversalSource g = graph.traversal();
    //Object result = g.V().limit(100).values().fold().next();            
    //Object result = null;
    //result = g.V().limit(100);
    //graph.io(IoCore.graphson()).write().iterate();
    
    String label = "UTXO";
    String property = "address";
    String value = "t1NGuQeRWKSBEqz9B35HoUiYEwpzxJc7w7p_";
    String index = "address_index";
    long startTime = System.currentTimeMillis();
    List<Vertex> vs_1 = g.V().hasLabel(label).has(property,value).in().toList();
    List<Vertex> vs_2 = g.V().hasLabel(label).has(property,value).out().out().toList();
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    System.out.println("Execution time: " + duration + " milliseconds");
    //List<Vertex> vs = g.V().has(property,value).toList();
    for (Vertex vertex : vs_1) {
        Object properties = g.V(vertex.id()).valueMap().next();
        System.out.println(properties);
    }
    System.out.println("---------------next----------------");
    for (Vertex vertex : vs_2) {
        Object properties = g.V(vertex.id()).valueMap().next();
        System.out.println(properties);
    }

    graph.close();
    }
}
