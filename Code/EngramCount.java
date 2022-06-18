//Java Imports needed for the code to run
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;


public class EngramCount {
	
  public static class EngramMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    //creating the objects to be used by the map
	//word1 to word3 are used to hold the values for the engram
	//intwritable holds the data for the occurence
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String word1 = null;
    private String word2 = null;
    private String word3 = null;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //Tokenised strings without punctiation
	  StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("\\p{Punct}",""));

		//word variables are nullified to avoid creating bad engrams
          word1 = null;
          word2 = null;
          word3 = null;
		  
		  //checking if tokens exist in order to fill the word variables
          if(itr.hasMoreTokens()){
                  word1 = itr.nextToken();
          }
          
          if(itr.hasMoreTokens()){
                          word2 = itr.nextToken();
                  }
                if(itr.hasMoreTokens()){
                        word3 = itr.nextToken();
                        word.set(word1 + " " + word2 + " " + word3);
                        context.write(word, one);
                }
      while (itr.hasMoreTokens()) {
                //updating word values and adding new token to word3
				word1 = word2;
                word2 = word3;
                word3 = itr.nextToken();
				
				//adding the words in the word variable
                word.set(word1 + " " + word2 + " " + word3);
		//returning the engram
		context.write(word, one);
      }
    }
  }
  
  public static class EngramMapper1
       extends Mapper<Object, Text, Text, IntWritable>{

	//creating the objects to be used by the map
	//word holdw the values for the unigram
	//intwritable holds the data for the occurence
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //Tokenised strings without punctiation
	  StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("\\p{Punct}",""));

      while (itr.hasMoreTokens()) {
		  //updating word value
		  word.set(itr.nextToken());
		 //returning the engram
		context.write(word, one);
      }
    }
  }
  
  public static class EngramMapper2
       extends Mapper<Object, Text, Text, IntWritable>{
	
	//creating the objects to be used by the map
	//word1 and word2 are used to hold the values for the engram
	//intwritable holds the data for the occurence
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
    private String word1 = null;
    private String word2 = null;
  
  public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //Tokenised strings without punctiation
	  StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("\\p{Punct}",""));
	  
	  //word variables are nullified to avoid creating bad engrams
		  word1 = null;
          word2 = null;
		  
          //checking if tokens exist in order to fill the word variables
		  if(itr.hasMoreTokens()){
                  word1 = itr.nextToken();
          }
          
          if(itr.hasMoreTokens()){
            word2 = itr.nextToken();
			word.set(word1 + " " + word2);
            context.write(word, one);
          }
               
      while (itr.hasMoreTokens()) {
        //updating word values and adding new token to word2
		word1 = word2;
        word2 = itr.nextToken();
				
		//adding the words in the word variable 
        word.set(word1 + " " + word2);
		//returning the engram
		context.write(word, one);
      }
    }
  }
  
  public static class EngramMapper4
       extends Mapper<Object, Text, Text, IntWritable>{

    //creating the objects to be used by the map
	//word1 to word4 are used to hold the values for the engram
	//intwritable holds the data for the occurence
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String word1 = null;
    private String word2 = null;
    private String word3 = null;
	private String word4 = null;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //Tokenised strings contain punctuations, e.g. comma, and fullstop
	  StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("\\p{Punct}",""));
      
	  //word variables are nullified to avoid creating bad engrams
          word1 = null;
          word2 = null;
          word3 = null;
		  word4 = null;
		  
		  //checking if tokens exist in order to fill the word variables
          if(itr.hasMoreTokens()){
                  word1 = itr.nextToken();
          }
          
          if(itr.hasMoreTokens()){
                          word2 = itr.nextToken();
                  }
                if(itr.hasMoreTokens()){
                        word3 = itr.nextToken();
                }
				
		if(itr.hasMoreTokens()){
            word4 = itr.nextToken();
            word.set(word1 + " " + word2 + " " + word3 + " " + word4);
            context.write(word, one);
        }
      while (itr.hasMoreTokens()) {
                //updating word values and adding new token to word4
				word1 = word2;
                word2 = word3;
                word3 = word4;
				word4 = itr.nextToken();
				
				//adding the words in the word variable 
                word.set(word1 + " " + word2 + " " + word3 + " " + word4);
		
		//returning the engram
		context.write(word, one);
      }
    }
  }
  
  public static class EngramMapper5
       extends Mapper<Object, Text, Text, IntWritable>{

    //creating the objects to be used by the map
	//word1 to word5 are used to hold the values for the engram
	//intwritable holds the data for the occurence
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String word1 = null;
    private String word2 = null;
    private String word3 = null;
	private String word4 = null;
	private String word5 = null;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		//Tokenised strings without punctiation
      StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("\\p{Punct}",""));
	  
	  //word variables are nullified to avoid creating bad engrams
          word1 = null;
          word2 = null;
          word3 = null;
		  word4 = null;
		  word5 = null;
		  
		  //checking if tokens exist in order to fill the word variables
          if(itr.hasMoreTokens()){
                  word1 = itr.nextToken();
          }
          
          if(itr.hasMoreTokens()){
                word2 = itr.nextToken();
          }
          if(itr.hasMoreTokens()){
                word3 = itr.nextToken();
          }
			
		  if(itr.hasMoreTokens()){
            word4 = itr.nextToken();
                        
          }
				
		  if(itr.hasMoreTokens()){
            word5 = itr.nextToken();
            word.set(word1 + " " + word2 + " " + word3 + " " + word4 + " " + word5);
            context.write(word, one);
           }
      while (itr.hasMoreTokens()) {
                //updating word values and adding new token to word5
				word1 = word2;
                word2 = word3;
                word3 = word4;
				word4 = word5;
				word5 = itr.nextToken();
				
				//adding the words in the word variable 
                word.set(word1 + " " + word2 + " " + word3 + " " + word4 + " " + word5);
		//returning the engram
		context.write(word, one);
      }
    }
  }
  
    public static class EngramReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
		   
	//writables and sum in order to make avoid creation of new Objects when the reduce method is reactivated
    private IntWritable result = new IntWritable();
    private int sum;

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      //calculating the number of occurrences
	  sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  
  public static class alphabeticPartitionerV2 extends Partitioner<Text,IntWritable>{
	  //custom partitioner class used to globally alphabetically sort the files inputted
                    
          private int asciiCode = 0;
		  //ASCII code is used in order to accelerate the process of calculating the number of the character
          
          public int getPartition(Text key, IntWritable value, int numReduceTasks){
                        
            asciiCode = key.toString().toLowerCase().charAt(0);
                        
						
			//Current configuration runs for 26 reducers
            if(numReduceTasks==0)
                return 0;
            if(Character.isDigit(key.toString().toLowerCase().charAt(0)) || asciiCode % 97 == 0){
                return 0;
            } else if (asciiCode % 97 >= numReduceTasks || asciiCode < 97){
                return numReduceTasks-1;
            } else {
                return Math.abs((asciiCode % 97));
            }
		  }
  }

  public static void main(String[] args) throws Exception {
    //setting up the hadoop configuration
	//this includes the slow start configuration that allows for the reducers
	//to kick in before mapping has been concluded
	//speculative execution for both the mapper and reducer, thus avoiding the job slowing down
	//compress the mappers output in order to accelerate the process
	//Increasing the memory used by fs and as mapper heap in order to accelerate the reading process
	//Specifying the maxsize for the split blocks to 128 MB which is the optimal setting
	//Setting the numtasks for JVM to -1 to reuse the JVMs created
	Configuration conf = new Configuration();
	conf.set("mapreduce.map.output.compress", "true");
	conf.set("mapreduce.map.tasks.speculative.execution","true");
	conf.set("mapreduce.reduce.tasks.speculative.execution","true");
	conf.set("mapred.output.compress.type", "BLOCK");	
	conf.set("mapred.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
	conf.set("mapred.job.reduce.slowstart.completedmaps", "0.15");
	conf.set("mapreduce.map.combine.minspills","6");
	conf.set("fs.inmemory.size.mb","512");
	conf.set("mapreduce.task.io.sort.mb","512");
	conf.set("mapreduce.input.fileinputformat.split.maxsize","134217728");
	conf.set("mapreduce.job.jvm.numtasks", "-1");
	
	//Setting up the job configurations
	//These include the setting up of the combiner
	//using the reducer class for reducer and combiner
	//using the custom partitioner class needed to globally alphabetically the output
	//and the output key and value classes
	//setting the  number of reducers to 26 in order to work properly
    Job job = Job.getInstance(conf, "engram count");
    job.setJarByClass(EngramCount.class);
	job.setCombinerClass(EngramReducer.class);
    job.setReducerClass(EngramReducer.class);
	job.setPartitionerClass(alphabeticPartitionerV2.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
	job.setNumReduceTasks(26);
	
	//Default value for the engram created by the program
	int ngrams = 3;
	
	//receiving the size of the engrams created and checking if it is a valid number
	try{
		ngrams = Integer.parseInt(args[2]);
	} catch(Exception e){
		ngrams = 3;
	}
	
	//allows to switch between the different mappers for each engram
	//the default choice is 3-grams
	//this configuration allows up to 5-grams to be created
	switch(ngrams){
		case 1:
			job.setMapperClass(EngramMapper1.class);
			break;
		case 2:
			job.setMapperClass(EngramMapper2.class);
			break;
		case 4:
			job.setMapperClass(EngramMapper4.class);
			break;
		case 5:
			job.setMapperClass(EngramMapper5.class);
			break;
		default:
			job.setMapperClass(EngramMapper.class);
			break;
	}
	
	/*Sets the parameter for combining the texr input in larger files
	* this allows for an optimized processing of the files inputted
	* it allows for a new way to calculate the number of splits in order to have an optimal number of
	* files to be processed.
	* It also allows to input the folder of the files needed to be processed
	* and the directory location where to output the file*/
	
    job.setInputFormatClass(CombineTextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

