import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.scaladsl.{FileIO, Framing, Keep, Sink, Source}
import akka.util.ByteString

import java.io.File
import java.nio.file.Paths
import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.StdIn.readLine
import scala.util.{Failure, Success}

object main {

  def getListOfFiles(dir: String): Seq[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(f => f.isFile && f.getName.endsWith(".csv")).map(_.getAbsolutePath).toSeq
    }
    else
      List[String]()
  }

  def processReadings(readings: Seq[List[String]], sensor: String): Seq[Int] = {

    def validateReading(maybeNumber: String): Int = {
      if (maybeNumber.map(_.isDigit).forall(identity)) {
        val reading = maybeNumber.toInt
        if (reading >= 0 || reading <= 100)
          reading
        else
          0
      } else {
        0
      }
    }

    readings.flatMap(_.filterNot(_ == sensor).map { r => validateReading(r) })
  }

  def FormatData(data: Seq[List[String]]): FinalResult = {
    var numFailed = 0
    val grouped = data.groupBy(_.head)

    val readings = grouped.keys.map { sen =>
      grouped.get(sen) match {
        case Some(readings) =>
          val readingProcessed = processReadings(readings, sen)

          numFailed += readingProcessed.count(_ == 0)

          val successReads = readingProcessed.filter(_ > 0)

          if (successReads.nonEmpty) {

            val (max, avg, min) = (successReads.max, successReads.sum.toDouble / successReads.length, successReads.min)

            Result(sen, min, avg, max)

          } else {
            Result(sen, 0, 0, 0)
          }
        case None => Result("", 0, 0, 0)
      }
    }.toList.filterNot(_.sensor == "")

    val sortedReadings = readings.sortWith(_.avg > _.avg)

    FinalResult(sortedReadings, data.length, numFailed)
  }

  @tailrec
  def GetDirectory(): Seq[String] = {
    print("Enter the directory Absolute path to read files: ")
    val directoryName = readLine()
    val maybeFilesList = getListOfFiles(directoryName)
    if (maybeFilesList.nonEmpty)
      maybeFilesList
    else {
      println("Provided Directory Path Incorrect/ No file with required Format")
      GetDirectory()
    }
  }

  def ProcessFiles(sources: Seq[Source[ByteString, Future[IOResult]]])(implicit system: ActorSystem, ec: ExecutionContext): Future[Seq[List[String]]] =
    Source(sources)
      .flatMapConcat(identity)
      .map(_.utf8String)
      .runWith(Sink.seq)
      .map { f =>
        f.filterNot(_.contains("sensor-id"))
          .map(_.split(",").toList)
      }

  def ReadFiles(filePaths: Seq[String])(implicit system: ActorSystem, ec: ExecutionContext): Seq[Source[ByteString, Future[IOResult]]] = {
    filePaths
      .map(Paths.get(_))
      .map(p =>
        FileIO
          .fromPath(p)
          .viaMat(Framing.delimiter(ByteString(System.lineSeparator()), Int.MaxValue, allowTruncation = true))(Keep.left)
      )
  }

  case class Result(sensor: String, min: Int, avg: Double, max: Int)

  case class FinalResult(results: List[Result], totalProcessed: Int, failValues: Int)

  def PrintStats(finalResult: FinalResult, processedFiles: Int): List[Unit] = {
    println(s"Num of processed files: $processedFiles")
    println(s"Num of processed measurements: ${finalResult.totalProcessed}")
    println(s"Num of failed measurements: ${finalResult.failValues}")
    println("sensor-id,min,avg,max")
    finalResult.results.map { r =>
      if (r.max != 0) {
        println(s"${r.sensor},${r.min},${String.format("%.2f", r.avg)},${r.max}")
      } else println(s"${r.sensor},NaN,NaN,NaN")
    }
  }

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem()
    implicit val ec: ExecutionContext = system.dispatcher
    println("::::::::::::::Start here:::::::::::::: \n")
    val filePaths = GetDirectory()
    val readFiles = ReadFiles(filePaths)
    val processFile = ProcessFiles(readFiles)
    val printer = for {
      processedFiles <- processFile
    } yield {
      FormatData(processedFiles)
    }
    printer.onComplete {
      case Failure(e) => println(s"Failed With message: $e")
      case Success(finalResult) => PrintStats(finalResult, filePaths.length)
        println("\n::::::::::::::End here:::::::::::::")
    }
    Await.ready(printer, Duration.Inf)
    print("")


  }
}
