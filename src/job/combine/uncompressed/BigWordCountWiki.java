package job.combine.uncompressed;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import util.split.SplitTable;



public class BigWordCountWiki {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  //params: /LijieXu/Wikipedia/enwiki-20110405.txt /Output/Wikipedia
  public static void main(String[] args) throws Exception {
	
	//long GB = 1024 * 1024 * 1024;
	//long totalDataSize = 1 * GB;
			
	int reduceNumArray[] = {9, 18};
	int splitSizeMBArray[] = {64, 128, 256};
	int xmxArray[] = {1000, 2000, 3000, 4000};
	int xmsArray[] = {0, 1};
	int ismbArray[] = {200, 400, 600, 800};
	
	for(int splitIndex = 0; splitIndex < splitSizeMBArray.length; splitIndex++) {
		for(int reduceNumIndex = 0; reduceNumIndex < reduceNumArray.length; reduceNumIndex++) {
			for(int xmxIndex = 0; xmxIndex < xmxArray.length; xmxIndex++) {
				for(int xmsIndex = 0; xmsIndex < xmsArray.length; xmsIndex++) {
					for(int ismbIndex = 0; ismbIndex < ismbArray.length; ismbIndex++) {
						
						int reduceNum = reduceNumArray[reduceNumIndex];
						int splitMB = splitSizeMBArray[splitIndex];
						int xmx = xmxArray[xmxIndex];
						int xms = xmsArray[xmsIndex] * xmx;
						int ismb = ismbArray[ismbIndex];
						
						Configuration conf = new Configuration();
						   
						conf.setLong("mapred.min.split.size", SplitTable.getMapred_min_split_size(splitMB));
						conf.setLong("mapred.max.split.size", SplitTable.getMapred_max_split_size(splitMB));
								
						//conf.setInt("my.sample.split.num", (int) (totalDataSize / (splitMB * 1024 * 1024)));
						
						conf.setInt("mapred.reduce.tasks", reduceNum);
						conf.setInt("io.sort.mb", ismb);
						
						if(xms == 0)
							conf.set("mapred.child.java.opts", "-Xmx" + xmx + "m");
						else
							conf.set("mapred.child.java.opts", "-Xmx" + xmx + "m -Xms" + xms + "m");
						
						conf.setInt("child.monitor.metrics.seconds", 2);
						conf.setInt("child.monitor.jvm.seconds", 2);
						conf.setInt("child.monitor.jstat.seconds", 2);
						
						
						
					    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
					    if (otherArgs.length != 2) {
					      System.err.println("Usage: BigWordCountWiki <in> <out>");
					      System.exit(2);
					    }
					    Job job = new Job(conf, "BigWordCountWiki " + splitMB + "MB " 
					    		+ conf.get("mapred.child.java.opts") 
					    		+ " ismb=" + ismb + " RN=" + reduceNum);
					    job.setJarByClass(BigWordCountWiki.class);
					    job.setMapperClass(TokenizerMapper.class);
					    job.setCombinerClass(IntSumReducer.class);
					    job.setReducerClass(IntSumReducer.class);
					    job.setOutputKeyClass(Text.class);
					    job.setOutputValueClass(IntWritable.class);
					    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
					    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
				
					    FileSystem.get(conf).delete(new Path(otherArgs[1]), true);
					    
					    job.waitForCompletion(true);
					    
					    Thread.sleep(15000);
					}
				}
			}
		}
	}
	
  }
}

