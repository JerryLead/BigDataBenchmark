package job.uncombine.uncompressed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BigReduceJoin extends Configured implements Tool {

	public static class JoinRecordMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, TextPair, TextPair> {
		private String pageRank;
		private TextPair p = new TextPair();
		private TextPair v = new TextPair();

		public void map(LongWritable key, Text value,
				OutputCollector<TextPair, TextPair> output, Reporter reporter)
				throws IOException {

			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			pageRank = itr.nextToken();
			p.set(pageRank, "0");
			v.set(value.toString(), "0");

			output.collect(p, v);
		}
	}

	public static class JoinStationMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, TextPair, TextPair> {

		private String pageRank;
		private TextPair p = new TextPair();
		private TextPair v = new TextPair();

		public void map(LongWritable key, Text value,
				OutputCollector<TextPair, TextPair> output, Reporter reporter)
				throws IOException {

			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			pageRank = itr.nextToken();
			p.set(pageRank, "1");
			v.set(value.toString(), "1");

			output.collect(p, v);
		}

	}

	public static class JoinReducer extends MapReduceBase implements
			Reducer<TextPair, TextPair, Text, Text> {
		public void reduce(TextPair key, Iterator<TextPair> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			Text outVal = new Text();

			List<Text> t1 = new ArrayList<Text>();
			Text tag = key.getSecond();

			TextPair value = null;
			while (values.hasNext()) {
				value = values.next();
				if (value.getSecond().compareTo(tag) == 0) {
					t1.add(value.getFirst());
				} else {
					/*
					 * for(Text val : t1) {
					 * outVal.set(value.getFirst().toString());
					 * //output.collect(pagerank, outVal); }
					 */
				}
			}

		}
	}

	public static class ConsumingJoinReducer extends MapReduceBase implements
			Reducer<TextPair, TextPair, Text, Text> {
		public void reduce(TextPair key, Iterator<TextPair> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			Text outVal = new Text();

			List<String> t1 = new ArrayList<String>();
			Text tag = key.getSecond();

			TextPair value = null;
			while (values.hasNext()) {
				value = values.next();
				if (value.getSecond().compareTo(tag) == 0) {
					t1.add(value.getFirst().toString());
				} else {
					/*
					 * for(Text val : t1) {
					 * outVal.set(value.getFirst().toString());
					 * //output.collect(pagerank, outVal); }
					 */
				}
			}

		}
	}

	public static class KeyPartitioner implements
			Partitioner<TextPair, TextPair> {
		@Override
		public void configure(JobConf job) {

		}

		@Override
		public int getPartition(TextPair key, TextPair value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE)
					% numPartitions;
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage: <ncdc input> <station input> <output>");
			return -1;
		}
		JobConf conf = new JobConf(getConf(), getClass());
		conf.setJobName("PageRank self join");

		Path ncdcInputPath = new Path(args[0]);
		Path stationInputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);

		MultipleInputs.addInputPath(conf, ncdcInputPath, TextInputFormat.class,
				JoinRecordMapper.class);
		MultipleInputs.addInputPath(conf, stationInputPath,
				TextInputFormat.class, JoinStationMapper.class);
		FileOutputFormat.setOutputPath(conf, outputPath);

		conf.setPartitionerClass(KeyPartitioner.class);
		conf.setOutputValueGroupingComparator(FirstComparator.class);

		conf.setMapOutputKeyClass(TextPair.class);
		conf.setMapOutputValueClass(TextPair.class);
		// conf.setReducerClass(JoinReducer.class);
		conf.setReducerClass(ConsumingJoinReducer.class);
		conf.setOutputKeyClass(Text.class);

		conf.setInt("mapred.reduce.tasks", 4);
		conf.setInt("io.sort.mb", 400);

		// conf.set(
		// "mapred.child.java.opts",
		// "-Xmx1000m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -verbose:gc -XX:-PrintGCDetails -XX:+PrintGCTimeStamps");
		conf.set("mapred.child.java.opts", "-Xmx1000m");
		// conf.setInt("child.monitor.metrics.seconds", 2);
		// conf.setInt("child.monitor.jvm.seconds", 2);
		conf.setInt("child.monitor.jstat.seconds", 2);

		conf.setInt("mapred.job.reuse.jvm.num.tasks", 1);
		// conf.setFloat("mapred.job.shuffle.input.buffer.percent", 0.9f);
		// conf.setFloat("mapred.job.shuffle.merge.percent", 0.9f);
		// conf.set("mapred.job.tracker", "local");
		// conf.set("fs.default.name", "file:///");

		FileSystem.get(conf).delete(outputPath, true);

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new BigReduceJoin(), args);
		System.exit(exitCode);

	}

	public static class TextPair implements WritableComparable<TextPair> {
		private Text first;
		private Text second;

		public TextPair() {
			set(new Text(), new Text());
		}

		public TextPair(String first, String second) {
			set(new Text(first), new Text(second));
		}

		public TextPair(Text first, Text second) {
			set(first, second);
		}

		public void set(Text first, Text second) {
			this.first = first;
			this.second = second;
		}

		public void set(String f, String s) {
			first.set(f);
			second.set(s);
		}

		public Text getFirst() {
			return first;
		}

		public Text getSecond() {
			return second;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			first.write(out);
			second.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			first.readFields(in);
			second.readFields(in);
		}

		@Override
		public int hashCode() {
			return first.hashCode() * 163 + second.hashCode();
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof TextPair) {
				TextPair tp = (TextPair) o;
				return first.equals(tp.first) && second.equals(tp.second);
			}
			return false;
		}

		@Override
		public String toString() {
			return first + "\t" + second;
		}

		@Override
		public int compareTo(TextPair tp) {
			int cmp = first.compareTo(tp.first);
			if (cmp != 0) {
				return cmp;
			}
			return second.compareTo(tp.second);
		}
	}

	public static class FirstComparator extends WritableComparator {
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

		public FirstComparator() {
			super(TextPair.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1])
						+ readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2])
						+ readVInt(b2, s2);
				return TEXT_COMPARATOR
						.compare(b1, s1, firstL1, b2, s2, firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			if (a instanceof TextPair && b instanceof TextPair) {
				return ((TextPair) a).first.compareTo(((TextPair) b).first);
			}
			return super.compare(a, b);
		}
	}
}
