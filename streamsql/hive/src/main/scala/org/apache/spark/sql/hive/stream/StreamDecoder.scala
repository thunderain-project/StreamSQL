package org.apache.spark.sql.hive.stream

import java.io.InputStream
import org.apache.hadoop.fs.Path
//import scala.collection.JavaConvertions._
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import akka.util.ByteString
import org.apache.hadoop.io.{ Writable, ByteWritable, BytesWritable }


trait SocketDecoder {
   // for socketDStream
  def streamToIterator(stream: InputStream): Iterator[Writable] = {
    new Iterator[Writable] {
      override def next(): Writable = {
        val data = new Array[Byte](1)
        stream.read(data)
        new ByteWritable(data(0))
      }
      override def hasNext(): Boolean = {
        if (stream.available > 0) {
          true
        } else {
          stream.close()
          false
        }
      }
    }
  }
}

trait ZeroMqDecoder {
  // for zeroMqInputDStream
  def bytesToObject(bytes: Seq[ByteString]): Iterator[Writable] = {
    bytes.iterator.map { bytestring =>
      new BytesWritable(bytestring.toArray) 
    }
  }
}

class KafkaDecoder(props: VerifiableProperties = null) extends Decoder[Writable] {
  // for kafkaInputDstream
  def fromBytes(bytes: Array[Byte]): Writable = {
    new BytesWritable(bytes)
  }
}

trait InputFilter {
  def pathFilter (path: Path): Boolean = {
    !path.getName().startsWith(".")
  }
}

trait StreamDecoder extends SocketDecoder with ZeroMqDecoder

object DefaultStreamDecoder extends StreamDecoder
object DefaultInputFilter extends InputFilter


