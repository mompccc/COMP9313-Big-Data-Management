package comp9313.ass2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import java.net.URI;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Test {

    public static String OUT = "output";
    public static String IN = "input";
    public static String QueryNode;
    public static enum COUNTER {
        UPDATE
    };
    public static class FirstMapper extends Mapper<Object, Text, LongWritable, Text> {

        
        private LongWritable word = new LongWritable();
        private Text tail = new Text();
        HashMap<String, String> map = new HashMap<String, String>();
        public static enum COUNTER {
            UPDATE
        };
        @Override
       public void map(Object key, Text value, Context context) 
               throws IOException, InterruptedException {
            
            String[] words = value.toString().split(" ");
            String from_node = words[1];
            String to_node = words[2];
            String dist = words[3];
            String temp_string;
            if(to_node.equals(QueryNode)){
                temp_string = "0.0|" + from_node + ":" + dist;
            }else{
                temp_string = "-1.0|" + from_node + ":" + dist;
            }
            
            if(map.containsKey(to_node)){
                String keys = map.get(to_node);
                String temp = keys + "," + from_node + ":" + dist;
                map.put(to_node, temp);
            }else{
                map.put(to_node, temp_string);
                }
        }
        
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Iterator<Map.Entry<String, String>> data = map.entrySet().iterator();
            while(data.hasNext()){
                Map.Entry<String, String> entry = data.next();
                word.set(Long.parseLong(entry.getKey()));
                tail.set(entry.getValue() + "|-");
                context.write(word, tail);
            }
        }
    }


    public static class FirstReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    public static class MainMapper extends Mapper<Object, Text, LongWritable, Text> {
        
        private LongWritable head = new LongWritable();
        private Text tail = new Text();
        @Override
        public void map(Object key, Text value, Context context) 
               throws IOException, InterruptedException {
            
            String[] line = value.toString().split("\t");
            String current_node = line[0];
            String last = line[1];
            String[] sp = last.split("\\|");
            
            head.set(Long.parseLong(current_node));
            tail.set(last);
            context.write(head, tail);
            
            if(!sp[0].equals("-1.0") && !sp[1].equals("-")){
                String[] more_nodes = sp[1].split(",");
                for(String coins: more_nodes){
                    if(!coins.isEmpty()){
                        String temp_node = coins.split(":")[0];
                        Double temp_dist = Double.parseDouble(coins.split(":")[1]);
                        Double updated = Double.parseDouble(sp[0]) + temp_dist;
                        head.set(Long.parseLong(temp_node));
                        tail.set(current_node + ":" + updated.toString());
                        context.write(head, tail);
                        System.out.println("map:"+head.toString()+","+tail.toString());
                    }
                }
            }
        }
    }
    
    public static class MainReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            String temp1 = "";
            Double min_dist;
            ArrayList<String> dists = new ArrayList<>();
            for(Text val: values){
                String str = val.toString();
                if(str.indexOf("|") != -1){
                    temp1 = str;
                }else{
                    dists.add(str);
                }
            }
            
            if(temp1.isEmpty()){
                temp1 = "-1.0|-|-"; 
            }
            
            String current_dist = temp1.split("\\|")[0];
            if (current_dist.equals("-1.0"))
                min_dist = Double.POSITIVE_INFINITY;
            else
                min_dist = Double.parseDouble(current_dist);
            
            String[] params = temp1.split("\\|");
            String coins = params[1];
            String last = params[2];
            
            String new_last = last;
            boolean updated = false;
            for (String pairs : dists) {
                String[] pair = pairs.split(":");
                String from = pair[0];
                double val = Double.parseDouble(pair[1]);
                if (val < min_dist) {
                    min_dist = val;
                    new_last = from;
                    updated = true;
                    context.getCounter(COUNTER.UPDATE).increment(1);
                }
            }
            
            String output = "";
            String minStr = min_dist.toString();
            if(min_dist == Double.POSITIVE_INFINITY){
                minStr = "-1.0";
            }
            output += (minStr + "|");
            output += (coins + "|");
            if (updated && !minStr.equals("-1.0")){
                output += new_last;
            }else if(!updated && !minStr.equals("-1.0")){
                output += new_last;
            }else{
                output += "-";
            }
            context.write(key, new Text(output));
        }
    }
    
    public static class OutMapper extends Mapper<Object, Text, LongWritable, Text> {
        
        private LongWritable head = new LongWritable();
        private Text tail = new Text();
        HashMap<String, String> map1 = new HashMap<String, String>();
        HashMap<String, String> map2 = new HashMap<String, String>();
        @Override
        public void map(Object key, Text value, Context context) 
               throws IOException, InterruptedException {
            
            String[] line = value.toString().split("\\t");
            String current_node = line[0];
            String current_dist = line[1].split("\\|")[0];
            String current_last = line[1].split("\\|")[2];

            map1.put(current_node, current_dist);
            map2.put(current_node, current_last);
        }
        
        protected void cleanup(Context context) throws IOException, InterruptedException {
            
            for (Entry<String, String> entry : map1.entrySet()) {
                String key = entry.getKey();
                ArrayList<String> temp_list = new ArrayList<>();
                String temp_tail = map1.get(key) + "\t";
                String node = key;
                temp_list.add(key);
                node = map2.get(node);
                while(!node.equals("-")){
                    temp_list.add("->" + node);
                    node = map2.get(node);
                }
                while(!temp_list.isEmpty()){
                    temp_tail += temp_list.get(0);
                    temp_list.remove(0);
                }
                head.set(Long.parseLong(key));
                tail.set(temp_tail);
                context.write(head, tail);
            }
        }
    }
    
    public static class OutReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            for (Text val : values) {
                String[] tokens = val.toString().split("\t");
                if (!tokens[0].equals("-1.0")){
                    context.write(key, val);
                }
            }
        }
    }
    
    public static void main(String[] args) throws Exception {        

        IN = args[0];
        OUT = args[1];
        QueryNode = args[2];

        int iteration = 0;

        String input = IN;
        String tmpOutput = OUT + "/../tmp/" + iteration++;
        
        Configuration conf = new Configuration();
        Job First = Job.getInstance(conf, "initialization");
        First.setJarByClass(Test.class);
        First.setMapperClass(FirstMapper.class);
        First.setReducerClass(FirstReducer.class);
        First.setOutputKeyClass(LongWritable.class);
        First.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(First, new Path(input));
        FileOutputFormat.setOutputPath(First, new Path(tmpOutput));
        First.waitForCompletion(true);

        boolean isdone = false;

        while (isdone == false) {                  
            input = tmpOutput;
            tmpOutput = OUT + "/../tmp/" + iteration++;

            Job Core = Job.getInstance(conf, "main");
            Core.setJarByClass(Test.class);
            Core.setMapperClass(MainMapper.class);
            Core.setReducerClass(MainReducer.class);
            Core.setOutputKeyClass(LongWritable.class);
            Core.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(Core, new Path(input));
            FileOutputFormat.setOutputPath(Core, new Path(tmpOutput));
            Core.waitForCompletion(true);
            
            Counters counters = Core.getCounters();
            long updates = counters.findCounter(COUNTER.UPDATE).getValue();
            System.out.println("Updates: " + updates);
            
            if (updates == 0){
                isdone = true;
            }
        }

        input = tmpOutput;
        tmpOutput = OUT;
        
        Job Done = Job.getInstance(conf, "last");
        Done.setJarByClass(Test.class);
        Done.setMapperClass(OutMapper.class);
        Done.setReducerClass(OutReducer.class);
        Done.setOutputKeyClass(LongWritable.class);
        Done.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(Done, new Path(input));
        FileOutputFormat.setOutputPath(Done, new Path(tmpOutput));
        Done.waitForCompletion(true);
        
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
        Path tmpDir = new Path("hdfs://localhost:9000/user/comp9313/tmp");
        if (fs.exists(tmpDir)){
            fs.delete(tmpDir, true);
        }
    }
}

