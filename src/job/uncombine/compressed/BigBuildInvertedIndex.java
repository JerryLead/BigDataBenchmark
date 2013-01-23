package job.uncombine.compressed;

/*
 * Cloud9: A MapReduce Library for Hadoop
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import util.split.SplitTable;

import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfWritables;
import edu.umd.cloud9.util.fd.Object2IntFrequencyDistributionEntry;
import edu.umd.cloud9.util.fd.Object2IntFrequencyDistribution;
import edu.umd.cloud9.util.pair.PairOfObjectInt;

@SuppressWarnings("deprecation")
public class BigBuildInvertedIndex extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(BigBuildInvertedIndex.class);

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, PairOfInts> {
		private static final Text word = new Text();
		private static final Object2IntFrequencyDistribution<String> termCounts = new Object2IntFrequencyDistributionEntry<String>();

		public void map(LongWritable docno, Text doc,
				OutputCollector<Text, PairOfInts> output, Reporter reporter)
				throws IOException {
			String text = doc.toString();
			termCounts.clear();

			String[] terms = text.split("\\s+");

			// First build a histogram of the terms.
			for (String term : terms) {
				if (term == null || term.length() == 0) {
					continue;
				}

				termCounts.increment(term);
			}

			// emit postings
			for (PairOfObjectInt<String> e : termCounts) {
				word.set(e.getLeftElement());
				output.collect(word,
						new PairOfInts((int) docno.get(), e.getRightElement()));
			}
		}
	}

	private static class MyReducer extends MapReduceBase
			implements
			Reducer<Text, PairOfInts, Text, PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>> {
		private final static IntWritable dfIntWritable = new IntWritable();

		public void reduce(
				Text key,
				Iterator<PairOfInts> values,
				OutputCollector<Text, PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>> output,
				Reporter reporter) throws IOException {
			ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();

			int df = 0;
			while (values.hasNext()) {
				postings.add(values.next().clone());
				df++;
			}

			dfIntWritable.set(df);
			output.collect(
					key,
					new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>(
							dfIntWritable, postings));
		}
	}

	private static int printUsage() {
		System.out.println("usage: [input-path] [output-path] [num-mappers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {

		
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
							
							JobConf conf = new JobConf(getConf(), BigBuildInvertedIndex.class);
							   
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

							conf.setJobName("BigBuildInvertedIndex " + splitMB + "MB " 
					    		+ conf.get("mapred.child.java.opts") 
					    		+ " ismb=" + ismb + " RN=" + reduceNum);

							String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
							if (otherArgs.length != 2) {
								System.err.println("Usage: BigBuildInvertedIndex <in> <out>");
								System.exit(2);
							}

							conf.setMapOutputKeyClass(Text.class);
							conf.setMapOutputValueClass(PairOfInts.class);
							conf.setOutputKeyClass(Text.class);
							conf.setOutputValueClass(PairOfWritables.class);
							SequenceFileOutputFormat.setOutputCompressionType(conf,
									CompressionType.BLOCK);
							conf.setOutputFormat(MapFileOutputFormat.class);

							conf.setMapperClass(MyMapper.class);
							// conf.setCombinerClass(IdentityReducer.class);
							conf.setReducerClass(MyReducer.class);
							FileInputFormat.setInputPaths(conf, new Path(otherArgs[0]));
							FileOutputFormat.setOutputPath(conf, new Path(otherArgs[1]));

							FileSystem.get(conf).delete(new Path(otherArgs[1]), true);
							
							
							try {
								JobClient.runJob(conf);
							} catch(IOException e) {
								e.printStackTrace();
							}
							Thread.sleep(15000);
				
						}			
					}	
				}		
			}	
		}
		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new BigBuildInvertedIndex(), args);
		System.exit(res);
	}
}
