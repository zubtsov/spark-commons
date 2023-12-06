package com.github.zubtsov.spark.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

public class BinaryFilesOutputFormat2 extends FileOutputFormat<Text, BytesWritable> {

    private static class BinaryFileRecordWriter extends RecordWriter<Text, BytesWritable> {

        private final Configuration configuration;
        private final TaskAttemptContext taskAttemptContext;

        public BinaryFileRecordWriter(Configuration configuration, TaskAttemptContext taskAttemptContext) {
            this.configuration = configuration;
            this.taskAttemptContext = taskAttemptContext;
        }

        @Override
        public void write(Text key, BytesWritable value) throws IOException {
            String filename = new File(key.toString()).getName();
            String filePath = FileOutputFormat.getOutputPath(taskAttemptContext) + "/" + filename; //FIXME: yes, there are much better ways to join paths
            try (OutputStream outputStream = FileSystem.get(configuration).create(new Path(filePath))) {
                outputStream.write(value.getBytes(), 0, value.getLength());
            }
        }

        @Override
        public void close(TaskAttemptContext context) {
            // Do nothing on close
        }
    }

    @Override
    public RecordWriter<Text, BytesWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) {
        return new BinaryFileRecordWriter(taskAttemptContext.getConfiguration(), taskAttemptContext);
    }
}
