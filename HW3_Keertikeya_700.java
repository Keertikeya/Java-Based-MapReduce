import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HW3_Keertikeya_700 {

	public static class FirstMapper extends Mapper<Object, Text, Text, IntWritable>{

	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	StringTokenizer itr = new StringTokenizer(value.toString());
	    	String uri;
	      
	    	while (itr.hasMoreTokens()) {
	    		uri = itr.nextToken();
	    		if(uri.startsWith("/") && !uri.endsWith("/"))
	    			context.write(new Text(uri), new IntWritable(1));
	      }
	    }
	}

	public static class FirstReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	    	int sum = 0;
	    	for (IntWritable val : values) {
	    		sum += val.get();
	    	}
	    	context.write(key, new IntWritable(sum));
	    }
	}
  
	public static class SecondMapper extends Mapper<Text, Text, IntWritable, Text> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(new IntWritable(Integer.parseInt(value.toString())), key);			
		}
	}

	public static class SecondReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
		public void reduce(IntWritable count,Iterable<Text> words, Context context) throws IOException, InterruptedException {
			for(Text word: words)
				context.write(word, count);
			}
	}
	


	public static void main(String[] args) throws Exception {
	    Configuration conf1 = new Configuration();
	    Job job1 = Job.getInstance(conf1, "URI Counter");
	    job1.setJarByClass(HW3_Keertikeya_700.class);
	    job1.setMapperClass(FirstMapper.class);
	    job1.setCombinerClass(FirstReducer.class);
	    job1.setReducerClass(FirstReducer.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job1, new Path("count_out"));
	    
	    job1.submit();	
	
	    Configuration conf2 = new Configuration();
	    Job job2 = Job.getInstance(conf2, "Result Sorter");
	    job2.setJarByClass(HW3_Keertikeya_700.class);
	    job2.setMapperClass(SecondMapper.class);
	    job2.setReducerClass(SecondReducer.class);
	    job2.setMapOutputKeyClass(IntWritable.class);
	    job2.setMapOutputValueClass(Text.class);
	    job2.setInputFormatClass(KeyValueTextInputFormat.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job2, new Path("count_out/part-r-00000"));
	    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
 
		if (job1.waitForCompletion(true)) {
			job2.submit();
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
  }
}