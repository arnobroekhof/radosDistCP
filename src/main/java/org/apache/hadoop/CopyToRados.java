package org.apache.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.job.CopyCounter;
import org.apache.hadoop.job.CopyMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CopyToRados extends Configured implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(CopyToRados.class);
    private static final String MAX_SPLIT_SIZE = "3000";
    private static final String MIN_SPLIT_SIZE = "0";
    private static final String TASK_TIMEOUT = "0";

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new CopyToRados(), args);
        System.exit(res);
    }

    @Override
    public int run(final String[] strings) throws Exception {

        logger.info("Setup job");
        // When implementing tool
        Configuration conf = this.getConf();
        conf.set("mapreduce.input.fileinputformat.split.maxsize", MAX_SPLIT_SIZE);
        conf.set("mapreduce.input.fileinputformat.split.minsize", MIN_SPLIT_SIZE);
        // set task to timeout

        logger.info("Setting taks timeout to: {} ", TASK_TIMEOUT);
        conf.set("mapreduce.task.timeout", TASK_TIMEOUT);

        // Create job
        Job job = Job.getInstance(conf);
        job.setJarByClass(CopyToRados.class);

        conf.set("ceph.config.file", "/etc/ceph/ceph.conf");
        conf.set("ceph.id", "admin");
        conf.set("ceph.pool", "data");

        // Input
        logger.info("Input file is: {}", strings[0]);
        FileInputFormat.addInputPath(job, new Path(strings[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // Setup MapReduce job
        // Do not specify the number of Reducer
        job.setMapperClass(CopyMapper.class);
        job.setNumReduceTasks(0);

        // Specify key / value
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        // Output
        logger.info("Output file is: {}", strings[1]);
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        boolean result = job.waitForCompletion(true);
        // Execute job and return status
        logger.info("Finished copying files: {} success and {} failed",
            job.getCounters().findCounter(CopyCounter.FINISHED).getValue(),
            job.getCounters().findCounter(CopyCounter.FAILED).getValue());
        return result ? 0 : 1;
    }
}