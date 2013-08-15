package filter;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import util.split.SplitTable;

public class RankingsFilter {

	public static class TokenizerMapper extends
			Mapper<Object, Text, NullWritable, Text> {

		private final Text val = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			int loc = value.toString().indexOf('|');

			int rank = Integer.parseInt(value.toString().substring(0, loc)
					.trim()) % 3 + 1;

			String newVal = rank + value.toString().substring(loc);
			val.set(newVal);
			context.write(null, val);

		}
	}

	// params: /LijieXu/Wikipedia/enwiki-20110405.txt /Output/Wikipedia
	public static void main(String[] args) throws Exception {

		// long GB = 1024 * 1024 * 1024;
		// long totalDataSize = 1 * GB;

		Configuration conf = new Configuration();

		conf.setInt("mapred.reduce.tasks", 0);
		conf.setInt("io.sort.mb", 500);

		conf.set("mapred.child.java.opts", "-Xmx2000m");

		conf.setInt("child.monitor.jstat.seconds", 2);

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: RankingsFilter <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "RankingsFilter");
		job.setJarByClass(RankingsFilter.class);
		job.setMapperClass(TokenizerMapper.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		FileSystem.get(conf).delete(new Path(otherArgs[1]), true);

		job.waitForCompletion(true);

	}
}
