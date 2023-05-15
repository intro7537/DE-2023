import java.io.IOException;
import java.util.*;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.TextStyle;


import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20191059
{
	public static class DayOfDate
	{
		LocalDate date;
		DayOfWeek dayOfWeek;
		
		DayOfDate ( String date )
		{
			StringTokenizer itr = new StringTokenizer(date.toString(), "/");
			
			int month = Integer.parseInt(itr.nextToken().trim());
			int day = Integer.parseInt(itr.nextToken().trim());	
			int year = Integer.parseInt(itr.nextToken().trim());
			
			this.date = LocalDate.of(year, month, day);
			this.dayOfWeek = this.date.getDayOfWeek();
		}
		
		public String getDay()
		{
			String day = this.dayOfWeek.getDisplayName(TextStyle.SHORT, Locale.US);
			return day.toUpperCase();
		}
	}

	public static class TripsVehiclesMapper extends Mapper<Object, Text, Text, Text>
	{
		private Text one_word = new Text();
		private Text one_value = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			if (itr.countTokens() < 4) return;
			String region = itr.nextToken().trim();
			String date = itr.nextToken().trim();
			String vehicles = itr.nextToken().trim();
			String trips = itr.nextToken().trim();
			
			DayOfDate dayofdate = new DayOfDate(date);
			String day = dayofdate.getDay();
			
			one_word.set(region + ',' + day);
			one_value.set(trips + ',' + vehicles);
			context.write(one_word, one_value);
			
		}
	}

	public static class TripsVehiclesReducer extends Reducer<Text,Text,Text,Text> 
	{
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			int sumTrips = 0;
			int sumVehicles = 0;

			for (Text val : values) 
			{
				String oneLineVal = val.toString();
				StringTokenizer itr = new StringTokenizer(oneLineVal.toString(), ",");
				sumTrips += Integer.parseInt( itr.nextToken().trim() );
				sumVehicles += Integer.parseInt( itr.nextToken().trim() );
			}
			result.set(Integer.toString(sumTrips) + ',' + Integer.toString(sumVehicles));
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: TripsVehicles <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "trips&vehicles count");
		job.setJarByClass(UBERStudent20191059.class);
		job.setMapperClass(TripsVehiclesMapper.class);
		job.setCombinerClass(TripsVehiclesReducer.class);
		job.setReducerClass(TripsVehiclesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
