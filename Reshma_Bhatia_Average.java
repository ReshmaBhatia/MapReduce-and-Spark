import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Reshma_Bhatia_Average {
  public static class Reshma_Bhatia_AverageMapper
       extends Mapper<LongWritable, Text, Text, Text>{
    private final static Text event = new Text();
    private final static Text page_count = new Text();
    public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
        String line = val.toString();
		String[] tokens = line.split(",");
		if (!tokens[0].equals("id")){
			String eve = tokens[3];
			eve = eve.replaceAll("'","");
			eve = eve.replaceAll("-","");
			eve = eve.replaceAll("\\p{Punct}"," ");
			String pn = tokens[18];
			eve=eve.trim().toLowerCase();
			if (eve.equals("")) return;
			eve=eve.replaceAll("[^a-zA-Z0-9_ ]","");
			//Pattern pt = Pattern.compile("[^a-zA-Z0-9]");
			//Matcher match= pt.matcher(eve);
			//while(match.find())	eve=eve.replace(Character.toString(eve.charAt(match.start())),"");
			event.set(eve);
			page_count.set(pn);
			context.write(event,page_count);
		}
		
    }
  }
  public static class Reshma_Bhatia_AverageReducer extends Reducer<Text,Text,Text,Text> {
    //private IntWritable result = new IntWritable();
 public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
        int count=0;
		long page_sum=0;
		
		for (Text t:values){
            page_sum+=Integer.parseInt(t.toString());
            count+=1;
        }
        StringBuilder sb=new StringBuilder();
        double average = (double)page_sum / count; 
		sb.append(count + "\t" + String.format("%.3f", average));
        context.write(key, new Text(sb.toString()));
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "Reshma_Bhatia_Average");
    job.setJarByClass(Reshma_Bhatia_Average.class);
    job.setMapperClass(Reshma_Bhatia_AverageMapper.class);
    job.setReducerClass(Reshma_Bhatia_AverageReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}