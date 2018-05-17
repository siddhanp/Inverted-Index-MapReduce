    import java.io.IOException;
    import java.util.StringTokenizer;
    import java.util.HashMap;
    import java.util.Collections;
    import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.fs.Path;
    import org.apache.hadoop.io.LongWritable;
    import org.apache.hadoop.io.Text;
    import org.apache.hadoop.mapreduce.Job;
    import org.apache.hadoop.mapreduce.Mapper;
    import org.apache.hadoop.mapreduce.Reducer;
    import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
    import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

    public class InvertedIndex {

      public static class TokenizerMappers
           extends Mapper<LongWritable, Text, Text, Text>{

        private Text word = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
              
	      String[] w = value.toString().split("\t");
              Text dId = new Text(w[0]);
              if(w.length > 1) 
	      {
                  StringTokenizer itr = new StringTokenizer(w[1]);
                  System.out.print(dId.toString());
                  while (itr.hasMoreTokens()) 
		  {
                      word.set(itr.nextToken());
                      context.write(word, dId);
                  }
              }
        }
      }
  public static class IntSumReducers extends Reducer<Text,Text,Text,Text> {

          public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
              HashMap<String, Integer> hmap = new HashMap<>();

              for (Text v : values) {
                  hmap.put(v.toString(), 1+hmap.getOrDefault(v.toString(),0));
              }

              StringBuilder result = new StringBuilder();
              for(String keyString: hmap.keySet()){

                  result.append(keyString).append(":");
                  result.append(hmap.get(keyString)).append("\t");

              }
              
              Text op = new Text(result.toString());
              context.write(key, new Text(op));
          }
      }
      
      public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(TokenizerMappers.class);
        job.setReducerClass(IntSumReducers.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
    }