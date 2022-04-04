import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;

import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.*;
import org.graphframes.GraphFrame;
import org.graphframes.lib.AggregateMessages;
import scala.collection.mutable.WrappedArray;

import java.io.IOException;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class FindPath {

    // From: https://stackoverflow.com/questions/3694380/calculating-distance-between-two-points-using-latitude-longitude
    private static double distance(double lat1, double lat2, double lon1, double lon2) {
        final int R = 6371; // Radius of the earth
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c * 1000; // convert to meters
        double height = 0; // For this assignment, we assume all locations have the same height.
        distance = Math.pow(distance, 2) + Math.pow(height, 2);
        return Math.sqrt(distance);
    }
    static boolean runOnCluster = false;
    //https://stackoverflow.com/questions/50871891/how-to-parse-xml-using-databricks-in-spark-java

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("FindPath");
        int slices = 0;
        JavaSparkContext jsc = null;
        if (!runOnCluster) {
            sparkConf.setMaster("local[2]");
            sparkConf.setJars(new String[] { "target/eduonix_spark-deploy.jar" });
            slices = 10;
            jsc = new JavaSparkContext(sparkConf); }
        else {
            slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
            jsc = new JavaSparkContext(sparkConf);
        }
        SparkSession spark = SparkSession.builder().config(jsc.getConf()).getOrCreate();
        spark.sparkContext().setCheckpointDir("/tmp/checkpoints");
        spark.sparkContext().setLogLevel("WARN");
        spark.udf().register("mapdistance", (Double lat1, Double lat2, Double lon1, Double lon2) ->{
            final int R = 6371; // Radius of the earth
            double latDistance = Math.toRadians(lat2 - lat1);
            double lonDistance = Math.toRadians(lon2 - lon1);
            double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                    + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                    * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
            double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            double distance = R * c * 1000; // convert to meters
            double height = 0; // For this assignment, we assume all locations have the same height.
            distance = Math.pow(distance, 2) + Math.pow(height, 2);
            return Math.sqrt(distance);
        }, DataTypes.DoubleType);

        spark.udf().register("addpath", (String s, Long id) -> s.length() == 0 ? id.toString() : s + " -> " + id, DataTypes.StringType);

        String input1 = "data/NUS.osm";
//        spark.sqlContext().udf().register( "2wayArrayJoiner", )
        //args[0];
        String input2 = "data/input.txt";//args[1];
        String output1 = "output/result.adjmap.txt";//args[2];
        String output2 = "output/result.txt";//args[3];
//        FileSystem.
        StructType nodeSchema = new StructType(new StructField[] {
            new StructField("_id", DataTypes.LongType, false, Metadata.empty()),
            new StructField("_lat", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("_lon", DataTypes.DoubleType, false, Metadata.empty())
        });

        Dataset<Row> nodes = spark.read()
                .format("xml")
                .option("rootTag", "osm")
                .option("rowTag", "node")
                .schema(nodeSchema)
                .load(input1)
                .withColumnRenamed("_id", "id")
                .withColumnRenamed("_lat", "lat")
                .withColumnRenamed("_lon", "lon");
        nodes.registerTempTable("nodeTable");
//        nodes = spark.sql("SELECT _id, _lat, _lon FROM nodeTable ");
        // Added extra fields to ensure that _id, _timestamp, _uid is uniquely identifiable


        Dataset<Row> edges = spark.read()
                .format("xml")
                .option("rootTag", "osm")
                .option("rowTag", "way")
                .load(input1);
        edges.registerTempTable("edgesTable");
        edges = spark.sql("SELECT nd._ref as nd, array_contains(tag, named_struct(\"_VALUE\", string(null), \"_k\", \"oneway\", \"_v\", \"yes\")) as oneway FROM edgesTable WHERE array_contains(edgesTable.tag._k, 'highway') ");// WHERE tag IS NOT NULL");

        StructType edgeSchema = new StructType(new StructField[] {
                new StructField("src", DataTypes.LongType, false, Metadata.empty()),
                new StructField("dst", DataTypes.LongType, true, Metadata.empty()),
        });
        JavaRDD<Row> edgeRdd = edges.javaRDD().flatMap(
                (FlatMapFunction<Row, Row>) x -> {
                    boolean oneway=x.getBoolean(1);
                    List<Long> l =  x.getList(0);
                    ArrayList<Row> edgeList = new ArrayList<>();
                    for (int i= 0; i < l.size()-1 ; i++) {
                        Row r = RowFactory.create(l.get(i), l.get(i+1));
                        edgeList.add(r);
                        r = RowFactory.create(l.get(i), null);
                        edgeList.add(r);
                        if (!oneway) {
                            r = RowFactory.create(l.get(i+1), l.get(i));
                            edgeList.add(r);
                        }
                        if (i == l.size()-2) {
                            r = RowFactory.create(l.get(i+1), null);
                            edgeList.add(r);
                        }
                    }
                    return edgeList.iterator();
                });
        Dataset<Row> rawEdgeList = spark.createDataFrame(edgeRdd, edgeSchema).distinct();
        Dataset<Row> onlyNode = rawEdgeList.join(nodes, col("id").equalTo(rawEdgeList.col("src")).or(col("id").equalTo(rawEdgeList.col("dst"))), "left").select("id").distinct().withColumnRenamed("id", "n");
        // get all nodes
        Dataset<Row>  gnodes = onlyNode.join(nodes, col("n").equalTo(nodes.col("id")), "inner").drop("n");
        iterateEdges(rawEdgeList, output1, jsc);

        //  spark.sqlContext().dropTempTable("edgesTable");
        // src, dest
        // node schema: id, lat, lon
        // edge schema: src, dst, destLon, destLat, dist
        // double lat1, double lat2, double lon1, double lon2
        Dataset<Row> edge = rawEdgeList.join(gnodes.as("srcN"), rawEdgeList.col("src").equalTo(col("srcN.id")), "inner")
                                       .join(gnodes.as("destN"), rawEdgeList.col("dst").equalTo(col("destN.id")), "inner")
                .withColumn("cost", callUDF("mapdistance", col("srcN.lat"),  col("destN.lat"), col("srcN.lon"), col("destN.lon")))
                .select(col("src"), col("dst"), col("cost")).checkpoint();
        GraphFrame graph = GraphFrame.apply(gnodes, edge);
        shortestPathRunner(graph, input2, output2, jsc);
    }


    /**
     * Performs a dijkstra on the graph TODO: A* heuristic if possible
     * @param g
     * @param src
     * @param dest
     * @return
     */
    static String  shortestPath(GraphFrame g, Long src, Long dest) {
        //
        if (g.vertices().filter(col("id").equalTo(dest)).isEmpty()) {
            return "No path";
        }
//        Row destinationRow = g.vertices().where(col("id").equalTo(dest)).first();
        Dataset<Row> vertices = g.vertices()
                .withColumn("visited", lit(false))
                .withColumn("tcost", when(g.vertices().col("id").equalTo(src), 0).otherwise(Float.POSITIVE_INFINITY))
                .withColumn("path", lit(""));
        // src, dest
        // node schema: id, lat, lon visited, summedDist, path
        // edge schema: src, dst, destLon, destLat, dist
        // double lat1, double lat2, double lon1, double lon2

        Dataset<Row> cached = AggregateMessages.getCachedDataFrame(vertices);
        GraphFrame dijkstra = GraphFrame.apply(cached, g.edges());
        int i = 0;

        while (dijkstra.vertices().filter(col("visited").equalTo(false)).first()!= null) {

            long currNode = dijkstra.vertices().filter(col("visited").equalTo(false)).sort("tcost").first().getLong(0);
            System.out.println("Next Iteration: "+ i  + " Node: " + currNode);
            Column msgDistance = AggregateMessages.edge().getItem("cost").plus(AggregateMessages.src().getItem("tcost"));
            Column msgPath = callUDF("addpath", AggregateMessages.src().getItem("path"), AggregateMessages.src().getItem("id"));//callUDF("appendToPath", when(AggregateMessages.src().getItem("path").isNotNull(), AggregateMessages.src().getItem("path")), AggregateMessages.src().getItem("id"));
            Column msgForDest = when(AggregateMessages.src().getItem("id").equalTo(currNode), struct(msgDistance, msgPath));

            Dataset<Row> newDist = dijkstra.aggregateMessages()
                    .sendToDst(msgForDest)
                    .agg(min(AggregateMessages.msg()).alias("aggMess"));
//            newDist.printSchema();
//            newDist.filter(col("aggMess").isNotNull()).show(10);
            Column newVisitedCol = when(
                    dijkstra.vertices().col("visited").or(dijkstra.vertices().col("id").equalTo(currNode)), true).otherwise(false);

            Column newDistanceCol = when(newDist.col("aggMess").isNotNull().and(
                    newDist.col("aggMess").getItem("col1").lt(dijkstra.vertices().col("tcost"))), newDist.col("aggMess").getItem("col1"))
                    .otherwise(dijkstra.vertices().col("tcost"));
            Column newPathCol = when(newDist.col("aggMess").isNotNull().and(newDist.col("aggMess").getItem("col1").lt(dijkstra.vertices().col("tcost"))),
                    newDist.col("aggMess").getItem("col2").cast(DataTypes.StringType))
                    .otherwise(dijkstra.vertices().col("path").cast(DataTypes.StringType));
            Dataset<Row> newVertices = dijkstra.vertices()
                    .join(newDist, dijkstra.vertices().col("id").equalTo(newDist.col("id")), "left_outer")
                    .drop(newDist.col("id"))
                    .withColumn("newVisited", newVisitedCol)
                    .withColumn("newtcost", newDistanceCol)
                    .withColumn("newpath", newPathCol)
                    .drop("aggMess", "tcost", "path", "visited")
                    .withColumnRenamed("newtcost", "tcost")
                    .withColumnRenamed("newpath", "path")
                    .withColumnRenamed("newVisited", "visited");
//            newVertices.printSchema();
//            newVertices.sort("tcost").show(10);
            Dataset<Row> cachedNewVert;
            if (i % 20 == 0) {
                cachedNewVert = AggregateMessages.getCachedDataFrame(newVertices).localCheckpoint();
            } else if (i % 501 == 0) {
                cachedNewVert = AggregateMessages.getCachedDataFrame(newVertices).checkpoint();
            } else {
                cachedNewVert = AggregateMessages.getCachedDataFrame(newVertices);
            }
            dijkstra = GraphFrame.apply(cachedNewVert, dijkstra.edges());
            i++;
            if (dijkstra.vertices().filter(dijkstra.vertices().col("id").equalTo(dest)).first().getBoolean(3)) {
                return dijkstra.vertices().filter(dijkstra.vertices().col("id").equalTo(dest))
                        .withColumn("newpath",  callUDF("addpath", dijkstra.vertices().col("path"), dijkstra.vertices().col("id")))
                        .drop("visited", "path")
                        .first()
                        .getAs("newpath");
            }
        }
        return "No path";
    }
    static void writeOutput(String s, String output, Configuration c) {
        Path p = new Path(output);
        try {
            FileSystem fs = FileSystem.get(p.toUri(), c);
            fs.delete(p, true);
            fs.mkdirs(p.getParent());
            FSDataOutputStream stream = fs.create(p);
            stream.write(s.getBytes());
            stream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void shortestPathRunner(GraphFrame graphFrame, String input, String output, JavaSparkContext jsc) {
        Path p = new Path(input);
        Path pout = new Path(output);

        try {
            FileSystem fs = FileSystem.get(p.toUri(), jsc.hadoopConfiguration());
            Scanner sc = new Scanner(fs.open(p));
            List<Long> src = new ArrayList<>();
            List<Long> dest = new ArrayList<>();
            while (sc.hasNextLine()) {
                if (!sc.hasNextLong()) {
                    break;
                }
                src.add(sc.nextLong());
                dest.add(sc.nextLong());
            }
            sc.close();
            FSDataOutputStream stream = fs.create(pout);
            long startTime, endTime;

            for (int i = 0; i < src.size(); i++) {
                startTime = System.currentTimeMillis();
                String r = shortestPath(graphFrame, src.get(i), dest.get(i)) + "\n";
                stream.write(r.getBytes());
                endTime = System.currentTimeMillis();
                System.out.println("Time for iteration " + (endTime-startTime)/ 1000);
            }
            stream.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    static void iterateEdges(Dataset<Row> edges, String output, JavaSparkContext jsc) {
        Iterator<Row> w = edges.groupBy(col("src")).agg(collect_list("dst").as("adjacency")).orderBy("src").toLocalIterator();
        StringBuilder adjList = new StringBuilder();
        while (w.hasNext()) {
            Row r = w.next();
            Long src = r.getLong(0);
            List<Long> ed = r.getList(1);
            if (ed.size() == 0) {
                adjList.append(src).append(System.lineSeparator());
            } else {
                adjList.append(src).append(" ");
            }
            for (int i = 0; i < ed.size(); i++) {
                if (i == ed.size() - 1) {
                    adjList.append(ed.get(i)).append(System.lineSeparator());
                } else {
                    adjList.append(ed.get(i)).append(" ");
                }
            }
        }
        writeOutput(adjList.toString(), output, jsc.hadoopConfiguration());
    }
}
