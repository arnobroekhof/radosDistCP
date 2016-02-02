package org.apache.hadoop.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.rados.RadosConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ValidateMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private static final Logger logger = LoggerFactory.getLogger(ValidateMapper.class);

    private static final int BUFFER_SIZE = 2097152;
    private FileSystem fileSystem;
    private RadosConnection radosConnection;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        Configuration conf = context.getConfiguration();
        
        radosConnection = new RadosConnection(conf.get("ceph.config.file"), conf.get("ceph.id"), conf.get("ceph.pool"));
        fileSystem = FileSystem.get(context.getConfiguration());
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        logger.info("Map job key {} with value {}", key, value);
        String fileName = value.toString();

        Path hdfsPath = new Path(fileName);
        FileStatus[] statuses = fileSystem.listStatus(hdfsPath);
        for (FileStatus status : statuses) {
            logger.info("File status is OK: {}", status.getPath().getName());
            if (status.isDirectory()) {
                logger.info("File {} is a directory", status.getPath().getName());
            }
            if (status.isFile()) {
                logger.info("Start validating: {} with objectName: {}", status.getPath().getName(), fileName);

                ContentSummary contentSummary = fileSystem.getContentSummary(status.getPath());

                if (radosConnection.validate(fileName, contentSummary.getLength())) {
                    context.getCounter(ValidateCounter.MATCH).increment(1);
                } else {
                    context.getCounter(ValidateCounter.NOMATCH).increment(1);
                    context.write(key, value);
                }

                logger.info("Finished validating file: {}", fileName);

            }
        }
    }
}
