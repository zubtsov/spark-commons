package com.github.zubtsov.spark.data

import com.github.zubtsov.spark.SparkFunSuite
import org.apache.hadoop.io.{BytesWritable, NullWritable, Text}

import java.io.File
import scala.reflect.io.Directory
import BinaryFilesTest._

//TODO: assert that directories contain same files with the same content
class BinaryFilesTest extends SparkFunSuite {
  test("Test reading and writing binary file") {
    val outputFilesPath = "target/test-results/binary-files-test"

    new Directory(new File(outputFilesPath)).deleteRecursively()

    spark.read.format("binaryFile").load(inputFilesPath)
      //here you can do the processing
      .rdd.map(r => (new Text(r.getString(indexOfPathColumn)), new BytesWritable(r.getAs[Array[Byte]](indexOfContentColumn))))
      .saveAsNewAPIHadoopFile(outputFilesPath, classOf[NullWritable], classOf[BytesWritable], classOf[BinaryFilesOutputFormat])
  }

  test("Test reading and writing binary file 2") {
    val outputFilesPath = "target/test-results/binary-files-test2"

    new Directory(new File(outputFilesPath)).deleteRecursively()

    spark.read.format("binaryFile").load(inputFilesPath)
      //here you can do the processing
      .rdd.map(r => (new Text(r.getString(indexOfPathColumn)), new BytesWritable(r.getAs[Array[Byte]](indexOfContentColumn))))
      .saveAsNewAPIHadoopFile(outputFilesPath, classOf[NullWritable], classOf[BytesWritable], classOf[BinaryFilesOutputFormat2])
  }
}

object BinaryFilesTest {
  val inputFilesPath = "src/test/resources/BinaryFilesTest"
  val indexOfPathColumn = 0
  val indexOfContentColumn = 3
}