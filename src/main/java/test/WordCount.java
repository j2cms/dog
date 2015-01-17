package test;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
public class WordCount
{
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
         
        public void map(LongWritable key, Text value, Context context)
        {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens())
            {
                word.set(tokenizer.nextToken());
                try
                {
                    context.write(word, one);
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
        }
    }
     
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result = new IntWritable();
         
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
        {
            int sum = 0;
            for (IntWritable val : values)
            {
                sum += val.get();
            }
            result.set(sum);
            try
            {
                context.write(key, result);
            }
            catch (Exception e)// jdk 1.7  IOException | InterruptedException e
            {
                e.printStackTrace();
            }
        }
    }
     
    public static void main(String[] args)
    {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "192.168.1.110");
        //conf.set("yarn.resourcemanager.address", "192.168.1.110:8032");
        //conf.set("yarn.resourcemanager.scheduler.address", "192.168.1.110:8030");
         
        Job job = null;
        try
        {
            job = Job.getInstance(conf);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
         
        job.setJarByClass(WordCount.class);
         
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
         
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
         
        try
        {
            FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.110:9000/user/hadoop/input"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.110:9000/user/hadoop/output"));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
         
        try
        {
            job.submit();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
