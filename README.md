# Hadoop MapReduce Implementation

This is my cloud computing coursework. The goal of this coursework is to use Hadoop in the GCP to create engrams from Books.
The books are from a dataset of 10,000 text-based e-books pulled from [Project Gutenberg](http://www.gutenberg.org).

It should be noted that the goal is to optimize the code to successfully create engrams as fast as possible, this can be achieved from both using the code and modifying the settings used to create the cluster.

The runtime that was achieved was at 8 minutes for the 10000 books. It should be noted that without the modifications the processing time is closer to 3 and a half hours.

Multiple engrams can be generated using the code found in this repository. The sizes vary from unigrams to 5-grams. The execution time again varies from 5 minutes and 30 seconds to 9 minutes.

Global alphabetic sorting was also implemented, meaning that letters are organised alphabetically throughout the reducer output files.

Moreover, the program can detect if the number inputted is incorrect and use the default mapper instead, in this case it results in a 3-gram.

# Modifications performed to achieve better performance

## Designing the map function

One of the most important aspects of the MapReduce function is the mapper used to create the n-grams. My implementation was mainly focused on creating a design that with slight modifications can allow different sized n-grams to be mapped. We chose to use StringTokenizer as it allowed an easier way to know how many tokens were left in the string that was currently being mapped. Intermediate variables were used to make it possible to construct the n-grams.

Furthermore, objects were instantiated, and variables were declared at the class level and not in the methods to ensure that memory was used more efficiently since objects are constantly reused.

## Designing the partitioner

To ensure that all n-grams are globally sorted we have implemented a partitioner function. This function allocates each letter to a different reducer, although occasionally some rare characters end up being included in certain reducers. This method may not give equal load to all reducers but due to the increased number of reducers we have been able to cope with this model and no noticeable drop in performance has recorded seen.

## Optimizing the Hadoop execution

An important aspect of Hadoop execution is its optimization. Hadoop’s default settings are good when using a couple of small files but generally, they do not allow for the fastest execution when we need to go through 10000 books. To fix this and improve the running time, we have updated the default values used by Hadoop in its configuration files. It is advisable to include these settings in the cluster values but also in the Configuration object created for the job.
The first modification performed was the compression of the map output. This was done to save time when transferring information from the mapper to the reducer. Data is usually uncompressed needing more time to be written and read in the disc and then transferred to the reducer. Yet, because we used a compression algorithm less volume of data is needed to be transferred and thus it is faster. For the compression codec, we used the Snappy codec as it is one of the fastest in terms of execution times although the compression ratio is worse than Gzip. The keys modified were, mapreduce.map.output.compress set to true, mapred.output.compress.type set to BLOCK and mapred.map.output.compress.codec set to org.apache.hadoop.io.compress.SnappyCodec.

Next up was the use of a combiner, the combiner runs after the mapper and is used to pre-process the output of the mapper before it is moved to the reducer. Its job is to combine the output, reduce in a sense of the data that goes to the reducer and thus limiting the processing needed to be performed in the
reducer. This optimization technique requires us to set the combiner class, in this case, we use the same class with the reducer. This technique results in a significant increase in performance. When tested with 2 books, this technique resulted in a 30% reduction in processing time when initially tested. It reduced the running time from 9 seconds to 6 seconds when tested in a VM running Hadoop.

Another technique used for optimization was the use of speculative execution both in mappers and reducers. Speculative execution allows Hadoop to detect when a task is taking longer than usual to be completed and run a new task in its place while the existing task is also running[4]. In case the backup task finishes faster the original is killed and vice versa. The keys modified were mapreduce.map.tasks.speculative.execution and mapreduce.reduce.tasks.speculative.execution which were set to true.

To further optimize the execution the mapred.job.reduce.slowstart.completedmaps value was modified. This allows for the reduce function to start before the mapper has finished. Thus, resulting in a faster processing time as the reducer can run simultaneously with the mapper thus cutting down on the time needed for the reducer to run . The value used was 0.15, meaning that the reducer started when 15% of the mapper was completed. Yet, due to the optimizations performed the reducers kicked in around the 54% mark.
Furthermore, an increase in the in-memory filesystem used for the merge of the map-outputs at the reducers was performed, increasing the memory from 100 MB to 512 MB[7, 6]. This was done to be able to do the merger more efficiently. The key modified were fs.inmemory.size.mb which was set to 512 MB.
Memory was also increased in the heap memory of the mapper, which allows for less spilling of the files when it is completed, the value chosen was 512 MB. Originally, it was set at 2042 MB but that impacted the performance of the cluster when the size of books was increased and as such, it was first decreased to 1021 MB and then to 512 MB which was chosen.

It is worth noting that Hadoop generally underperforms when it has many small files to go through as those result in a large number of splits. As such the last optimization performed was done to decrease the number of splits and create larger files. That was done using CombineTextInputFormat class. Furthermore, the split size was set as a maximum at 128 MB, thus resulting in smaller but larger splits that accelerated the mapping and reduce procedures, the key used was mapreduce.input.fileinputformat.split.maxsize and its value was set to 134217728. The key mapreduce.job.jvm.numtasks was also set to -1 to indicate to Hadoop to reduce the JVMs created.
The Hadoop cluster used to process the 10 thousand books was set to the default values using the N1 series chip in standard configuration, the number of worker nodes was increased from 2 to 4 to allow for faster processing of the books.