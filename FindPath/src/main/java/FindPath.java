import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;

import org.apache.spark.sql.types.*;

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
        String input1 = "data/NUS.osm";
//        spark.sqlContext().udf().register( "2wayArrayJoiner", )
        //args[0];
//        String input2 = args[1];
         String output1 = "output/result.adjmap.txt";//args[2];
//        String output2 = args[3];
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
                .load(input1);
        nodes.printSchema();
        nodes.registerTempTable("nodeTable");
//        nodes = spark.sql("SELECT _id, _lat, _lon FROM nodeTable ");
        nodes.show(5);
        // Added extra fields to ensure that _id, _timestamp, _uid is uniquely identifiable


        Dataset<Row> edges = spark.read()
                .format("xml")
                .option("rootTag", "osm")
                .option("rowTag", "way")
                .load(input1);
        edges.registerTempTable("edgesTable");
        edges = spark.sql("SELECT nd._ref as nd, array_contains(tag._k, \"oneway\") AND array_contains(tag._v, \"yes\") as oneway FROM edgesTable WHERE array_contains(edgesTable.tag._k, 'highway') ");// WHERE tag IS NOT NULL");
        edges.printSchema();
        edges.show(5);
        StructType edgeSchema = new StructType(new StructField[] {
                new StructField("src", DataTypes.LongType, false, Metadata.empty()),
                new StructField("dest", DataTypes.LongType, false, Metadata.empty()),
        });
        JavaRDD<Row> edgeRdd = edges.javaRDD().flatMap(
                (FlatMapFunction<Row, Row>) x -> {
                    boolean oneway=x.getBoolean(1);
                    List<Long> l =  x.getList(0);
                    ArrayList<Row> edgeList = new ArrayList<>();
                    for (int i= 0; i < l.size()-1 ; i++) {
                        Row r = RowFactory.create(l.get(i), l.get(i+1));
                        edgeList.add(r);
                        if (!oneway) {
                            r = RowFactory.create(l.get(i+1), l.get(i));
                            edgeList.add(r);
                        }
                    }
                    return edgeList.iterator();
                });
        Dataset<Row> rawEdgeList = spark.createDataFrame(edgeRdd, edgeSchema);
        rawEdgeList.registerTempTable("edgeList");
        Iterator<Row> w = rawEdgeList.groupBy(col("src")).agg(collect_list("dest").as("adjacency")).toLocalIterator();
        StringBuilder adjList = new StringBuilder();
        while (w.hasNext()) {
            Row r = w.next();
            Long src = r.getLong(0);
            List<Long> ed = r.getList(1);
            adjList.append(src).append(" ");
            for (int i = 0; i < ed.size(); i++) {
                if (i == ed.size() - 1) {
                    adjList.append(ed.get(i)).append(System.lineSeparator());
                } else {
                    adjList.append(ed.get(i)).append(" ");
                }
            }
        }
        writeOutput(adjList.toString(), output1, jsc.hadoopConfiguration());

        //        GraphFrame graph = GraphFrame.apply(nodes, edges);
        //        https://stackoverflow.com/questions/39158954/how-to-create-a-simple-spark-graphframe-using-java
        //        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    }
    static void writeOutput(String s, String output, Configuration c) {
        Path p = new Path(output);
        try {
            FileSystem fs = FileSystem.get(p.toUri(), c);
            fs.mkdirs(p.getParent());
            FSDataOutputStream stream = fs.create(p);
            stream.write(s.getBytes());
            stream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
