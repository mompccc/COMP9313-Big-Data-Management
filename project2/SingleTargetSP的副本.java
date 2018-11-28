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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SingleTargetSP {

    public static String OUT = "output";
    public static String IN = "input";
    public static String QueryNode;
    public static String target_node;
    public static HashMap<String, String> Not_All = new HashMap<String, String>();
    public static ArrayList<String> Nodes = new ArrayList<>();
    public static ArrayList<String> Nodes2 = new ArrayList<>();
    
    public static class FirstMapper extends Mapper<Object, Text, LongWritable, Text> {

        private LongWritable word = new LongWritable();
        private Text tail = new Text();
        HashMap<String, String> map = new HashMap<String, String>();
        @Override
       public void map(Object key, Text value, Context context) 
               throws IOException, InterruptedException {
            
            String[] words = value.toString().split(" ");
            String from_node = words[1];
            String to_node = words[2];
            if(!Not_All.containsKey(to_node)){
                Not_All.put(to_node, null);
            }
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
            
            Configuration conf = context.getConfiguration();  
            String global_target = conf.get("Global_input");
            String[] line = value.toString().split("\\t");
            String current_node = line[0];
            Double current_dist = Double.parseDouble(line[1].split("\\|")[0]);
            String[] more_nodes = line[1].split("\\|")[1].split(",");
            head.set(Long.parseLong(current_node));
            tail.set(line[1]);
            context.write(head, tail);
            
            if(global_target.equals(current_node)){
                Nodes.remove(0);
                for(String coins: more_nodes){
                    String temp_node = coins.split(":")[0];
                    if(!Not_All.containsKey(temp_node)){
                        head.set(Long.parseLong(temp_node));
                        tail.set("-1.0|"+temp_node+":0.0|-");
                        context.write(head, tail);
                        Not_All.put(temp_node, null);
                    }
                    Double temp_dist = Double.parseDouble(coins.split(":")[1]);
                    Double updated = current_dist + temp_dist;
                    String node_to_update = temp_node;
                    if(!Nodes2.contains(node_to_update)){
                        Nodes.add(node_to_update);
                        Nodes2.add(node_to_update);
                    }
                    head.set(Long.parseLong(node_to_update));
                    tail.set(updated.toString());
                    context.write(head, tail);
                    System.out.println("map: "+ Nodes);
                }
            }
            
        }
    }
    
    public static class MainReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        
        private Text tail = new Text();
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            Configuration conf = context.getConfiguration();  
            String global_target = conf.get("Global_input");
            String temp1 = "";
            Double temp2 = -1.0;
            for(Text val: values){
                String str = val.toString();
                if(str.indexOf("|") != -1){
                    temp1 = str;
                }else{
                    temp2 = Double.parseDouble(str);
                }
            }

            Double dist = Double.parseDouble(temp1.split("\\|")[0]);
            if(dist==-1.0){
                dist = temp2;
                if(dist!=-1.0){
                    String fin = dist.toString() + "|" + temp1.split("\\|")[1] + "|" + global_target;
                    tail.set(fin);
                    context.write(key, tail);
                }else{
                    String fin = dist.toString() + "|" + temp1.split("\\|")[1] + "|-";
                    tail.set(fin);
                    context.write(key, tail);
                }
            }else if(temp2 < dist && dist != 0.0 && temp2 != -1.0){
                dist = temp2;
                String fin = dist.toString() + "|" + temp1.split("\\|")[1] + "|" + global_target;
                tail.set(fin);
                context.write(key, tail);
            }else{
                tail.set(temp1);
                context.write(key, tail);
            }           
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
                    //DecimalFormat formatted = new DecimalFormat("#.#");
                    //double dist = Double.valueOf(formatted.format(Double.parseDouble(tokens[0])));
                    //String output = dist + "\t" + tokens[1];
                    context.write(key, val);
                }
            }
        }
    }
    
    public static void main(String[] args) throws Exception {        

        IN = args[0];
        OUT = args[1];
        //QueryNode = "116";
        QueryNode = args[2];
        Nodes.add(QueryNode);

        int iteration = 0;

        String input = IN;
        String tmpOutput = OUT + "/../tmp/" + iteration++;
        
        Configuration conf = new Configuration();
        Job First = Job.getInstance(conf, "initialization");
        First.setJarByClass(SingleTargetSP.class);
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
            System.out.println(Nodes);

            target_node = Nodes.get(0);
            Nodes2.add(target_node);
            conf.set("Global_input", target_node);
            Job Core = Job.getInstance(conf, "main");
            Core.setJarByClass(SingleTargetSP.class);
            Core.setMapperClass(MainMapper.class);
            Core.setReducerClass(MainReducer.class);
            Core.setOutputKeyClass(LongWritable.class);
            Core.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(Core, new Path(input));
            FileOutputFormat.setOutputPath(Core, new Path(tmpOutput));
            Core.waitForCompletion(true);
            
            if(Nodes.isEmpty()){
                isdone = true;
            }
        }

        input = tmpOutput;
        tmpOutput = OUT;
        
        Job Done = Job.getInstance(conf, "last");
        Done.setJarByClass(SingleTargetSP.class);
        Done.setMapperClass(OutMapper.class);
        Done.setReducerClass(OutReducer.class);
        Done.setOutputKeyClass(LongWritable.class);
        Done.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(Done, new Path(input));
        FileOutputFormat.setOutputPath(Done, new Path(tmpOutput));
        Done.waitForCompletion(true);
        
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
        // delete existing directory
        Path tmpDir = new Path("hdfs://localhost:9000/user/comp9313/tmp");
        //if (fs.exists(tmpDir))
            //fs.delete(tmpDir, true);
    }
}

