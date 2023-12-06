package com.github.zubtsov.spark.data

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

import java.io.{File, OutputStream}

class BinaryFilesOutputFormat extends FileOutputFormat[Text, BytesWritable] {
  private class BinaryFileRecordWriter(configuration: Configuration, taskAttemptContext: TaskAttemptContext) extends RecordWriter[Text, BytesWritable] {

    override def write(key: Text, value: BytesWritable): Unit = {
      var outputStream: OutputStream = null

      try {
        val filename = new File(key.toString).getName
        val filePath = s"${FileOutputFormat.getOutputPath(taskAttemptContext)}/$filename"
        outputStream = FileSystem.get(configuration).create(new Path(filePath))
        outputStream.write(value.getBytes, 0, value.getLength)
      } finally {
        if (outputStream != null) {
          outputStream.close()
        }
      }
    }

    override def close(taskAttemptContext: TaskAttemptContext): Unit = ()
  }

  override def getRecordWriter(taskAttemptContext: TaskAttemptContext): RecordWriter[Text, BytesWritable] =
    new BinaryFileRecordWriter(taskAttemptContext.getConfiguration, taskAttemptContext)
}
