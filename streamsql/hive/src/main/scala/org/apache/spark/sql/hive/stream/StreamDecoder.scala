package org.apache.spark.sql.hive.stream

import java.io.InputStream
import org.apache.hadoop.fs.Path
//import scala.collection.JavaConvertions._
import kafka.serializer._
import kafka.utils.VerifiableProperties
import akka.util.ByteString
import org.apache.hadoop.io.{ Writable, ByteWritable, BytesWritable, Text }

trait SocketDecoder extends Serializable {
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

trait ZeroMqDecoder extends Serializable {
  // for zeroMqInputDStream
  def bytesToObject(bytes: Seq[ByteString]): Iterator[Writable] = {
    bytes.iterator.map { bytestring =>
      new BytesWritable(bytestring.toArray) 
    }
  }
}

trait KafkaDecoder extends Decoder[Writable] with Serializable {
  // for kafkaInputDstream
  def fromBytes(bytes: Array[Byte]): Writable = {
    new BytesWritable(bytes)
  }
}

class KafkaTextDecoder(props: VerifiableProperties = null) extends KafkaDecoder {
  
  override def fromBytes(bytes: Array[Byte]): Writable = {
    new Text(bytes)
  }
}

class KafkaStringDecoder(props: VerifiableProperties = null) extends Decoder[String] with Serializable {
  
  val encoding =
    if(props == null)
      "UTF8"
    else
      props.getString("serializer.encoding", "UTF8")

  def fromBytes(bytes: Array[Byte]): String = {
    new String(bytes, encoding)
  }
}

trait InputFilter extends Serializable {
  def pathFilter (path: Path): Boolean = {
    !path.getName().startsWith(".")
  }
}

trait StreamDecoder extends SocketDecoder with ZeroMqDecoder

object DefaultStreamDecoder extends StreamDecoder
object DefaultInputFilter extends InputFilter