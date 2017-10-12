package Cloud.ApacheLog;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

public class Runner {

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception
  {
        JobConf conf = new JobConf(Runner.class);
        conf.setJobName("ip-count");
        
        conf.setMapperClass(IpMapper.class);
        
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);
        
        conf.setReducerClass(IpReducer.class);
        
        
        // take the input and output from the command line
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]+"-stage"));

        JobClient.runJob(conf);

        JobConf sortconf = new JobConf(Runner.class);
        sortconf.setJobName("ip-count-sorter");
        
        sortconf.setMapperClass(SortMap.class);
        
        sortconf.setMapOutputKeyClass(IntWritable.class);
        sortconf.setMapOutputValueClass(Text.class);
        
        sortconf.setReducerClass(SortReduce.class);
        

        // take the input and output from the command line
        FileInputFormat.setInputPaths(sortconf, new Path(args[1]+"-stage"));
        FileOutputFormat.setOutputPath(sortconf, new Path(args[1]));

        Job sortJob = new Job(sortconf,"ip-count-desc-sort");
        sortJob.setSortComparatorClass(Comparer.class);
try{
	int code = sortJob.waitForCompletion(true) ? 0 : 1;
	System.exit(code);
}
catch(Exception e){
e.printStackTrace();
}

	}

}
