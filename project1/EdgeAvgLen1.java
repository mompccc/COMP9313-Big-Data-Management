package comp9313.ass1;  //this is a in individual combiner version

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EdgeAvgLen1 {

  public static class DistanceMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text word = new Text();
    private Text tail = new Text();

    public void map(Object key, Text value, Context context)
                    throws IOException, InterruptedException {

      String[] words = value.toString().split("\\ ");  //split each line by space into a words-list
      char one = '1';  //using "1" as counting for each node
      String node = words[2];  //we only consider nodes which have incoming distance
      String dist = words[3];  //the distance for each node can be found at row 3
      String v1 = one + "," + dist;  //combine the count and distance as pair(using string)
      word.set(node);
      tail.set(v1);
      context.write(word, tail);
      //System.out.println("Mapper输出: " + word + "+" + tail);
      }
    }

  public static class TheCombiner
    extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
                  throws IOException, InterruptedException {
    int count1 = 0;
    double sum1 = 0.0;
    for (Text val : values) {
      String[] list = val.toString().split("\\,");  //unzip the string-pair
      int tem1 = Integer.parseInt(list[0]);
      count1 += tem1;  //count the amount of incoming distance
      double tem2 = Double.parseDouble(list[1]);
      sum1 += tem2;  //compute the sun of distance
    }
    String str = Integer.toString(count1) + "," + Double.toString(sum1);  //combine them into string-pair
    result.set(str);
     context.write(key, result);
     //System.out.println("Combiner输出: " + "node" + key.toString() + result.toString());
    }
  }
  
  public static class AvgReducer
       extends Reducer<Text,Text,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<Text> values, Context context)
                       throws IOException, InterruptedException {
      int count = 0;
      double sum = 0.0;
      for (Text val : values) {
        String[] list = val.toString().split("\\,");  //unzip the string-pair
        int temp1 = Integer.parseInt(list[0]);
        count += temp1;
        double temp2 = Double.parseDouble(list[1]);
        sum += temp2;
      }
      double avg = sum/count;
      int t = (int) avg;
      if (t != 0 ){  //Remove the nodes that have no in-coming edges
        result.set(avg);
          context.write(key, result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "sum distance");
    job.setJarByClass(EdgeAvgLen1.class);
    job.setMapperClass(DistanceMapper.class);
    job.setReducerClass(AvgReducer.class);
    job.setCombinerClass(TheCombiner.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}