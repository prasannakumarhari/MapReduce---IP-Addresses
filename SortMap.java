package Cloud.ApacheLog;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Mapper that takes a line from an Apache access log and emits the IP with a
 * count of 1. This can be used to count the number of times that a host has
 * hit a website.
 */
public class SortMap extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> 
{

  // Regular expression to match the IP at the beginning of the line in an
  // Apache access log


  //public void sortmap(Text ipaddress, IntWritable count, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {  
  public void map(LongWritable fileOffset, Text lineContents,OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {  
        String line = lineContents.toString();  
        StringTokenizer tokenizer = new StringTokenizer(line);
	Logger.getLogger("BreakPoint - Line").info(line);
		String strIpAddress = ""; 
		String strCount = ""; 
	if(tokenizer.hasMoreTokens()){
		strIpAddress = tokenizer.nextToken().trim();
		Logger.getLogger("BreakPoint - IpAddress").info(strIpAddress);
	}
	if(tokenizer.hasMoreElements()){
		strCount = tokenizer.nextToken().trim();
		Logger.getLogger("BreakPoint - Count").info(strCount);
	}
	output.collect(new IntWritable(Integer.parseInt(strCount)),new Text(strIpAddress));
      }
 
}
