package fly.play.s3

import play.api.libs.iteratee._
import scala.concurrent._
import javassist.bytecode.ByteArray
import play.api.libs.concurrent.Execution.Implicits._
import scala.concurrent.duration._
import play.api.mvc.Results._
import play.api.mvc.BodyParser
import scala.util.{ Try, Success, Failure }
import play.api.mvc.SimpleResult
import play.api.mvc.RequestHeader
import play.api.mvc.SimpleResult
import play.api.mvc.SimpleResult

object S3BodyParser {

  // Chunking stuff
  private val partSize = ((5 * 1024) * 1024);

  private def qualifyChunks: Iteratee[Array[Byte], Array[Byte]] = {
    def step(c_buff: Array[Byte])(i: Input[Array[Byte]]): Iteratee[Array[Byte], Array[Byte]] = i match {
      case Input.EOF => Done(c_buff, Input.EOF)
      case Input.Empty => Cont[Array[Byte], Array[Byte]](i => step(c_buff)(i))
      case Input.El(e) =>
        val n_buff = c_buff ++ e
        n_buff.length match {
          case x if x > partSize => {
            val (done, next) = n_buff.splitAt(partSize)
            Done(done, Input.El(next))
          }
          case _ => Cont[Array[Byte], Array[Byte]](i => step(n_buff)(i))
        }
    }
    (Cont[Array[Byte], Array[Byte]](i => step(Array())(i)))
  }

  // public API

  def apply(bucket: Bucket, fileName: String) = {

  }

  // my own type

  private sealed trait ProcessingIntermediate

  private case class AdirectPutObject(fileName: String) extends ProcessingIntermediate

  private case class ACurrentMultipart(parts: List[BucketFilePartUploadTicket]) extends ProcessingIntermediate
  private case class AnError(s3error: S3Exception) extends ProcessingIntermediate

  // define my iteratee
  private def iterateeUploading(bfut: BucketFileUploadTicket, bucket: Bucket, keyName: String, contentType: String): Iteratee[Array[Byte], ProcessingIntermediate] = {
    def step(acc: ProcessingIntermediate)(input: Input[Array[Byte]]): Iteratee[Array[Byte], ProcessingIntermediate] = acc match {
      case AdirectPutObject(x) => Done(acc)
      case AnError(x) => Done(acc)
      case ACurrentMultipart(parts) => {
        input match {
          case Input.EOF => Done(acc)
          case Input.Empty => (Cont[Array[Byte], ProcessingIntermediate](i => step(acc)(i)))
          case Input.El(data) => {
            val ds = data.length
            Iteratee.flatten {
              if (ds == 0 && parts.isEmpty) {
                // ACL here To do
                (bucket.add(BucketFile(keyName, contentType, Array[Byte]())))
                  .recover({
                    case S3Exception(status, code, message, originalXml) => Done(AnError(S3Exception(status, code, message, originalXml)))
                  })
                  .map(
                    unit => Done(AdirectPutObject(keyName)))
              } else {
                (
                  bucket uploadPart (bfut, BucketFilePart(parts.size + 1, data))).recover({
                    case S3Exception(status, code, message, originalXml) => Done(AnError(S3Exception(status, code, message, originalXml)))
                  }).map(ticket => (Cont[Array[Byte], ProcessingIntermediate](i => step(ACurrentMultipart(parts :+ ticket.asInstanceOf[BucketFilePartUploadTicket]))(i))))
              }
            }
          }
        }
      }
    }
    (Cont[Array[Byte], ProcessingIntermediate](i => step(ACurrentMultipart(List[BucketFilePartUploadTicket]()))(i)))
  }

  // main function for iteratee usage
  private def uploadsChunks(fileName: String, contentType: String, bucket: Bucket): Iteratee[Array[Byte], Either[S3Exception, PointerToBucketFile]] = {

    val keyName = fileName
    Iteratee.flatten {
      bucket.initiateMultipartUpload(BucketFile(fileName, contentType)).map(bfut => {
        Enumeratee.grouped[Array[Byte]](qualifyChunks) &>> iterateeUploading(bfut, bucket, keyName, contentType).map(iterateeResult => iterateeResult match {
          case AnError(s3error) => Left(s3error)
          case AdirectPutObject(po) =>
            Right(PointerToBucketFile(po, contentType))
          case ACurrentMultipart(parts) => {
            val r = bucket.completeMultipartUpload(bfut, parts)
              .recover({
                case S3Exception(status, code, message, originalXml) => (Left(S3Exception(status, code, message, originalXml)))
              })
              .map(
                unit => (Right(PointerToBucketFile(keyName, contentType))))
            Await.result(r, 5 minutes)
          }
        })
      })
    }
  }

  // first bodyparser
  private def getBodyParser(fileName: String, contentType: String, bucket: Bucket) = BodyParser(rh => uploadsChunks(fileName, contentType, bucket).mapDone(r => {
    val rr: Either[SimpleResult, PointerToBucketFile] = r match {
      case Right(r) => Right(r)
      case Left(s3ex) => Left(InternalServerError)
    }
    rr
  }))

  

}




