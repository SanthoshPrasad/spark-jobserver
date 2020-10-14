package spark.jobserver.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import akka.serialization.JSerializer
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.CommonMessages.JobStarted
import spark.jobserver.io
import spark.jobserver.io.{BinaryInfo, BinaryType, ErrorData, JobInfo}

/**
  * Akka Serialization extension for StartJob message to accommodate large config values (>64KB)
  */
class JobStartedSerializer extends JSerializer {
  val logger = LoggerFactory.getLogger(getClass)

  override def includeManifest() : Boolean = false

  override def identifier() : Int = 12377

  override def toBinary(obj: AnyRef): Array[Byte] = {
    logger.info(s"Serializing JobStarted object -- ${obj}")
    try {
      val byteArray = new ByteArrayOutputStream()
      val out = new ObjectOutputStream(byteArray)
      val jobStarted = obj.asInstanceOf[JobStarted]

      out.writeObject(jobStarted.jobId);

      out.writeObject(jobStarted.jobInfo.jobId)
      out.writeObject(jobStarted.jobInfo.contextId)
      out.writeObject(jobStarted.jobInfo.contextName)
      out.writeObject(jobStarted.jobInfo.classPath)
      out.writeObject(jobStarted.jobInfo.state)
      out.writeObject(jobStarted.jobInfo.startTime)
      out.writeObject(jobStarted.jobInfo.endTime.getOrElse(null))
      out.writeObject(jobStarted.jobInfo.error.getOrElse(null))

      // write binary info
      out.writeObject(jobStarted.jobInfo.binaryInfo.appName)
      out.writeObject(jobStarted.jobInfo.binaryInfo.binaryType.name)
      out.writeObject(jobStarted.jobInfo.binaryInfo.uploadTime)

      out.flush()
      byteArray.toByteArray
    } catch {
      case ex : Exception =>
        throw new IllegalArgumentException(s"Object of unknown class cannot be serialized " +
          s"${ex.getMessage}")
    }
  }

  override def fromBinaryJava(bytes: Array[Byte],
                              clazz: Class[_]): AnyRef = {
    logger.info(s"Deserializing JobStarted object -- ${bytes.length}")
    try {
      val input = new ByteArrayInputStream(bytes)
      val inputStream = new ObjectInputStream(input)

      val rootJobId = inputStream.readObject().asInstanceOf[String]
      val jobId = inputStream.readObject().asInstanceOf[String]
      val contextId = inputStream.readObject().asInstanceOf[String]
      val contextName = inputStream.readObject().asInstanceOf[String]
      val classPath = inputStream.readObject().asInstanceOf[String]
      val state = inputStream.readObject().asInstanceOf[String]
      val startTime = inputStream.readObject().asInstanceOf[DateTime]

      val endTimeObj = inputStream.readObject()
      var endTime: DateTime = null

      if(endTimeObj!=null){
        endTime = endTimeObj.asInstanceOf[DateTime]
      }

      val errorDataObj = inputStream.readObject()
      var errorData: ErrorData = null

      if(errorDataObj!=null){
        errorData = errorDataObj.asInstanceOf[ErrorData]
      }


      val appName = inputStream.readObject().asInstanceOf[String]
      val binaryTypeName = inputStream.readObject().asInstanceOf[String]
      val uploadTime = inputStream.readObject().asInstanceOf[DateTime]

      val binaryType = BinaryType.fromString(binaryTypeName)

      val binaryInfo = BinaryInfo(appName, binaryType, uploadTime)

     val jobStarted = JobStarted(rootJobId, io.JobInfo(jobId, contextId, contextName, binaryInfo,
       classPath, state, startTime, Option(endTime), Option(errorData)))

      logger.info(s"After Deserializing JobStarted object from byte array-- ${jobStarted}")

      jobStarted

    } catch {
      case ex: Exception =>
        throw new IllegalArgumentException(s"Object of unknown class cannot be deserialized " +
          s"${ex.getMessage}")
    }
  }
}