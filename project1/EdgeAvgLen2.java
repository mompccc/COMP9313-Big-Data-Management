package comp9313.ass1;  //this is a in-mapper combining version

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EdgeAvgLen2 {

  public static class DistanceMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text word = new Text();
    private Text tail = new Text();
    Map<String, String> map = new HashMap<String, String>();

    public void map(Object key, Text value, Context context)
                    throws IOException, InterruptedException {

      String[] words = value.toString().split("\\ ");  //split each line by space into a words-list
      char one = '1';  //using "1" as counting for each node
      String node = words[2];  //we only consider nodes which have incoming distance
      String dist = words[3];  
      String v1 = one + "," + dist;  //combine the count and distance as pair(using string)
      if (map.containsKey(node)){  //use hashmap to store data for each node
        String keys = map.get(node);
        String[] list = keys.toString().split("\\,");  //unzip string-pair
        int temp1 = Integer.parseInt(list[0]) + 1;
        double temp2 = Double.parseDouble(list[1]) + Double.parseDouble(dist);
        String fin = Integer.toString(temp1) + "," + Double.toString(temp2);
        map.put(node, fin);  //update the count and sum for nodes
      }else{
        map.put(node, v1);  //if new node appears, create key-value pair
      }
      //System.out.println("Mapper输出: " + node + "+" + map.get(node));
    }
      
    protected void cleanup(Context context) throws IOException, InterruptedException {
      
      Iterator<Map.Entry<String, String>> data = map.entrySet().iterator();
      while(data.hasNext()){  //use cluanup to pop all the node-data pair
        Map.Entry<String, String> entry = data.next();
        word.set(entry.getKey());
        tail.set(entry.getValue());
        context.write(word, tail);
        //System.out.println("Combiner输出: " + word + "+" + tail);
      }
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
        String[] list = val.toString().split("\\,");
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
    job.setJarByClass(EdgeAvgLen2.class);
    job.setMapperClass(DistanceMapper.class);
    job.setReducerClass(AvgReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}