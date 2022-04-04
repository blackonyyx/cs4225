package assign1_common_words;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class TopkCommonWords{
  public static class TokenizerMapper1 extends Mapper<Object, Text, Text, IntWritable> {
    private static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private HashSet<String> stopWords;

    @Override
    @SuppressWarnings("unchecked")
    protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
      URI[] cacheFiles = context.getCacheFiles();
      try {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path path = new Path(cacheFiles[0].toString());
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
        stopWords = new HashSet<>();
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
          stopWords.add(line);
        }
        fs.close();
      } catch (Exception e) {
        System.err.println("Error: Target File Cannot Be Read");
      }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), " \t\n\r\f");
      while (itr.hasMoreTokens()) {
        String tok = itr.nextToken();
        if (stopWords.contains(tok)) {
          continue;
        }
        word.set(tok);
        context.write(word, one);
      }
    }
  }

  public static class TokenizerMapper2 extends Mapper<Object, Text, Text, IntWritable> {
    private static IntWritable two = new IntWritable(2);
    private Text word = new Text();
    private HashSet<String> stopWords;
    @Override
    protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
      URI[] cacheFiles = context.getCacheFiles();
      try {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path path = new Path(cacheFiles[0].toString());
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
        stopWords = new HashSet<>();
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
          stopWords.add(line);
        }
        fs.close();
      } catch (Exception e) {
        System.err.println("Error: Target File Cannot Be Read");
      }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), " \t\n\r\f");
      while (itr.hasMoreTokens()) {
        String tok = itr.nextToken();
        if (stopWords.contains(tok)) {
          continue;
        }
        word.set(tok);
        context.write(word, two);
      }
    }
  }


  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum1 = 0;
      int sum2 = 0;
      for (IntWritable val : values) {
        if (val.get() == 1){
          sum1 += 1;
        } else {
          sum2 += 1;
        }
      }

      if (sum1 == 0 || sum2 == 0) {
        return;
      }
      result.set(Math.max(sum1, sum2));
      context.write(key, result);
    }
  }

  public static class IdentityMapper extends Mapper<Object, Text, Text, LongWritable> {
    LongWritable i = new LongWritable();
    Text v = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), " \t\n\r\f");
      String word;
      long val;
      while (itr.hasMoreTokens()) {
         word = itr.nextToken();
         val = Long.parseLong(itr.nextToken());
         i.set(val);
         v.set(word);
         context.write(v, i);
      }
    }
  }
  public static class IntSumPriorityQueue extends Reducer<Text,LongWritable, LongWritable, Text> {
    private class Tuple implements Comparable<Tuple> {
      String  t;
      Long val;
      Tuple(String t, long val) {
        this.t = t;
        this.val = val;
      }

      @Override
      public String toString() {
        return "Tuple{" +
                "t=" + t +
                ", val=" + val +
                '}';
      }

      @Override
      public int compareTo(Tuple o) {
        if (!this.val.equals(o.val)) {
          return this.val.compareTo(o.val);
        } else {
          return (this.t.compareTo(o.t));
        }
      }
    }
    private TreeSet<Tuple> set;

    @Override
    protected void setup(Reducer<Text, LongWritable, LongWritable, Text>.Context context) throws IOException, InterruptedException {
       set = new TreeSet<>(Tuple::compareTo);
    }

    public void reduce(Text key, Iterable<LongWritable> values, Context context) {
        List<LongWritable> listOfValues = new ArrayList<>();
        for (LongWritable i : values) {
//          System.out.println(key.toString()+ " val: "+ i);
          listOfValues.add(i);
        }
        LongWritable value = listOfValues.stream().max(LongWritable::compareTo).get();
//        System.out.println(listOfValues);
        // put the new tuple into the queue
        set.add(new Tuple(key.toString(), value.get()));
        if (set.size() > 20) {
          // pop the smallest conforming value
          set.remove(set.first());
        }
    }

    @Override
    protected void cleanup(Reducer<Text,LongWritable, LongWritable, Text>.Context context) throws IOException, InterruptedException {
//        System.out.println(set);

        for (Tuple t : set.descendingSet()) {
          context.write(new LongWritable(t.val), new Text(t.t));
        }
        super.cleanup(context);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String tmp = "assign1/tmp";

    Path output = new Path(tmp);
    FileSystem hdfs = FileSystem.get(conf);

    // delete existing directory
    if (hdfs.exists(output)) {
      hdfs.delete(output, true);
    }

    Job job1 = Job.getInstance(conf, "filter stopwords and count words");
    job1.addCacheFile(new Path(args[2]).toUri());
    job1.setJarByClass(TopkCommonWords.class);
    job1.setMapperClass(TokenizerMapper1.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);

    job1.setReducerClass(IntSumReducer.class);
    MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, TokenizerMapper1.class);
    MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, TokenizerMapper2.class);
    FileOutputFormat.setOutputPath(job1, new Path(tmp));
    job1.waitForCompletion(true);

    Job job2 = Job.getInstance(conf, "priorityqueue sort");
    job2.setJarByClass(TopkCommonWords.class);
    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    job2.setMapperClass(IdentityMapper.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(LongWritable.class);

    job2.setReducerClass(IntSumPriorityQueue.class);

    FileInputFormat.addInputPath(job2, new Path(tmp));
    FileOutputFormat.setOutputPath(job2, new Path(args[3]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}


