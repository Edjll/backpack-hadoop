package ru.edjll.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import ru.edjll.hadoop.format.input.BackpackInputFormat;
import ru.edjll.hadoop.type.map.input.KeyInputMapType;
import ru.edjll.hadoop.type.map.output.ValueOutputMapType;

public class BackpackStarter {

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(BackpackStarter.class);

        job.setJobName("Backpack");

        job.setOutputKeyClass(KeyInputMapType.class);
        job.setOutputValueClass(ValueOutputMapType.class);

        job.setMapperClass(BackpackMapper.class);
        job.setCombinerClass(BackpackReducer.class);
        job.setReducerClass(BackpackReducer.class);

        job.setInputFormatClass(BackpackInputFormat.class);

        BackpackInputFormat.setInputFilePath(job, new Path(args[0]));
        BackpackInputFormat.setLimit(job, Double.parseDouble(args[1]));

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
