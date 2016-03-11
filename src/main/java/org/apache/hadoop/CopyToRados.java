package org.apache.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.UUID;

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
    public int run(final String[] args) throws Exception {

        logger.info("Setup job");
        // When implementing tool
        Configuration conf = this.getConf();
        conf.set("mapreduce.input.fileinputformat.split.maxsize", MAX_SPLIT_SIZE);
        conf.set("mapreduce.input.fileinputformat.split.minsize", MIN_SPLIT_SIZE);

        // set task to timeout
        logger.info("Setting taks timeout to: {} ", TASK_TIMEOUT);
        conf.set("mapreduce.task.timeout", TASK_TIMEOUT);

        if (conf.get("ceph.conf.file").isEmpty()) {
            conf.set("ceph.conf.file", "/etc/ceph/ceph.conf");
        }

        if (conf.get("ceph.id").isEmpty()) {
            conf.set("ceph.id", "admin");
        }
        if (conf.get("ceph.pool").isEmpty()) {
            conf.set("ceph.pool", "data");
        }

        // Create job
        Job job = Job.getInstance(conf);
        job.setJarByClass(CopyToRados.class);

        String inputList = "/tmp/" + UUID.randomUUID();
        logger.info("Input list = {}", inputList);


        // Input
        logger.info("Input path is: {}", args[0]);
        createInputList(inputList, args[0], FileSystem.get(conf));
        Path inputFileList = new Path(inputList);
        FileInputFormat.addInputPath(job, inputFileList);
        job.setInputFormatClass(TextInputFormat.class);

        // Setup MapReduce job
        // Do not specify the number of Reducer
        job.setMapperClass(CopyMapper.class);
        job.setNumReduceTasks(0);

        // Specify key / value
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        String outputFile = "/tmp/" + UUID.randomUUID();
        // Output
        logger.info("Output file is: {}", outputFile);
        FileOutputFormat.setOutputPath(job, new Path(outputFile));
        job.setOutputFormatClass(TextOutputFormat.class);

        boolean result = job.waitForCompletion(true);
        // Execute job and return status
        logger.info("Finished copying files: {} success and {} failed",
            job.getCounters().findCounter(CopyCounter.FINISHED).getValue(),
            job.getCounters().findCounter(CopyCounter.FAILED).getValue());

        FileSystem.get(conf).delete(inputFileList, true);

        return result ? 0 : 1;
    }


    public void createInputList(final String inputList, final String path, final FileSystem fileSystem) throws IOException {

        Path inputListPath = new Path(inputList);
        if (fileSystem.exists(inputListPath)) {
            fileSystem.delete(inputListPath, true);
        }
        FSDataOutputStream fsDataOutputStream = fileSystem.create(inputListPath);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream));
        Path hdfsPath = new Path(path);
        Queue pathsToVisit = new ArrayDeque();
        pathsToVisit.add(hdfsPath);
        while (pathsToVisit.size() > 0) {
            Path curPath = (Path) pathsToVisit.remove();
            FileStatus[] statuses = fileSystem.listStatus(curPath);
            for (FileStatus status : statuses) {
                if (status.isDirectory()) {
                    pathsToVisit.add(status.getPath());
                }

                if (status.isFile()) {
                    bufferedWriter.write(status.getPath().toString() + "\n");
                }
            }
        }
        bufferedWriter.close();
        fsDataOutputStream.close();
    }
}