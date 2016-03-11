package org.apache.hadoop.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.rados.RadosConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CopyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private static final Logger logger = LoggerFactory.getLogger(CopyMapper.class);

    private static final int DEFAULT_BUFFER_SIZE = 2097152;
    private String bufferSize;
    private FileSystem fileSystem;
    private RadosConnection radosConnection;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        Configuration conf = context.getConfiguration();
        String cephConfigFile = conf.get("ceph.conf.file");
        String cephId = conf.get("ceph.id");
        String cephPool = conf.get("ceph.pool");
        bufferSize = conf.get("buffer.size", Integer.toString(DEFAULT_BUFFER_SIZE));

        logger.info("Using buffer size: {}", bufferSize );
        logger.info("Mapper using ceph config file: {}", cephConfigFile);
        logger.info("Mapper using ceph id: {}", cephId);
        logger.info("Mapper using ceph pool: {}", cephPool);

        radosConnection = new RadosConnection(cephConfigFile, cephId, cephPool);
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
                logger.info("Start copying file: {} with objectName: {}", status.getPath().getName(), fileName);

                FSDataInputStream inputStream = new FSDataInputStream(fileSystem.open(status.getPath()));

                try {
                    radosConnection.putObject(inputStream, fileName, Integer.parseInt(bufferSize));
                }
                catch (Exception e) {
                    context.getCounter(CopyCounter.FAILED).increment(1);
                    e.printStackTrace();
                }
                logger.info("Finished Copying file: {}", fileName);
                context.getCounter(CopyCounter.FINISHED).increment(1);
                logger.debug("Closing Hadoop inputstream");
                inputStream.close();
            }
        }
    }
}
