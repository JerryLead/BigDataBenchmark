package job.uncombine.uncompressed;



import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import util.split.SplitTable;

public class BigTwitterGraphReverser {

  public static class TokenizerMapper 
       extends Mapper<LongWritable, Text, Text, Text>{
    
    private final static Text sourceNode = new Text();
    private final static Text targetNode = new Text();
      
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      if(itr.hasMoreTokens()) {
    	  sourceNode.set(itr.nextToken());
          if(itr.hasMoreTokens()) {
        	  targetNode.set(itr.nextToken());
        	  context.write(targetNode, sourceNode);
          }
      }
    	 
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text, Text, Text, Text> {
    private final static Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      StringBuilder sb = new StringBuilder();
      
      for (Text val : values) {
        sb.append(val.toString() + "\t");
      }
      result.set(sb.toString());
      context.write(key, result);
    
    }
  }

  public static void main(String[] args) throws Exception {

	//long GB = 1024 * 1024 * 1024;
	//long totalDataSize = 1 * GB;
		
	int reduceNumArray[] = {9, 18};
	int splitSizeMBArray[] = {64, 128, 256};
	int xmxArray[] = {1000, 2000, 3000, 4000};
	int xmsArray[] = {0, 1};
	int ismbArray[] = {200, 400, 600, 800};	
		
	for(int reduceNumIndex = 0; reduceNumIndex < reduceNumArray.length; reduceNumIndex++) {
		for(int splitIndex = 0; splitIndex < splitSizeMBArray.length; splitIndex++) {
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
							System.err.println("Usage: BigTwitterGraphReverser <in> <out>");
							System.exit(2);
						}
					
						Job job = new Job(conf, "BigTwitterGraphReverser " + splitMB + "MB " 
					    		+ conf.get("mapred.child.java.opts") 
					    		+ " ismb=" + ismb + " RN=" + reduceNum);
						job.setJarByClass(BigTwitterGraphReverser.class);
						job.setMapperClass(TokenizerMapper.class);
	    
						job.setReducerClass(IntSumReducer.class);
						job.setOutputKeyClass(Text.class);
						job.setOutputValueClass(Text.class);
						
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
