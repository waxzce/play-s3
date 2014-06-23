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
import play.api.mvc.SimpleResult
import play.api.mvc.BodyParsers.parse
import play.api.mvc.BodyParsers.parse.Multipart.FileInfo
import java.util.UUID
import java.text.Normalizer

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

  // my own type

  private sealed trait ProcessingIntermediate

  private case class AdirectPutObject(fileName: String, length:Int) extends ProcessingIntermediate

  private case class ACurrentMultipart(parts: List[BucketFilePartUploadTicket], length:Int) extends ProcessingIntermediate
  private case class AnError(s3error: S3Exception) extends ProcessingIntermediate

  // define my iteratee
  private def iterateeUploading(bfut: BucketFileUploadTicket, bucket: Bucket, keyName: String, contentType: String, acl:Option[ACL]): Iteratee[Array[Byte], ProcessingIntermediate] = {
    def step(acc: ProcessingIntermediate)(input: Input[Array[Byte]]): Iteratee[Array[Byte], ProcessingIntermediate] = acc match {
      case AdirectPutObject(x,l) => Done(acc)
      case AnError(x) => Done(acc)
      case ACurrentMultipart(parts, length) => {
        input match {
          case Input.EOF => Done(acc)
          case Input.Empty => (Cont[Array[Byte], ProcessingIntermediate](i => step(acc)(i)))
          case Input.El(data) => {
            val ds = data.length
            Iteratee.flatten {
              if (ds == 0 && parts.isEmpty) {
                // ACL here To do
                (bucket.add(BucketFile(keyName, contentType, Array[Byte](), acl=acl)))
                  .recover({
                    case S3Exception(status, code, message, originalXml) => Done(AnError(S3Exception(status, code, message, originalXml)))
                  })
                  .map(
                    unit => Done(AdirectPutObject(keyName, ds)))
              }else if (ds < partSize-1 && parts.isEmpty) {
                // ACL here To do
                (bucket.add(BucketFile(keyName, contentType, data, acl=acl)))
                  .recover({
                    case S3Exception(status, code, message, originalXml) => Done(AnError(S3Exception(status, code, message, originalXml)))
                  })
                  .map(
                    unit => Done(AdirectPutObject(keyName, ds)))
              } else {
                (
                  bucket uploadPart (bfut, BucketFilePart(parts.size + 1, data))).recover({
                    case S3Exception(status, code, message, originalXml) => Done(AnError(S3Exception(status, code, message, originalXml)))
                  }).map(ticket => (Cont[Array[Byte], ProcessingIntermediate](i => step(ACurrentMultipart(parts :+ ticket.asInstanceOf[BucketFilePartUploadTicket], length + ds))(i))))
              }
            }
          }
        }
      }
    }
    (Cont[Array[Byte], ProcessingIntermediate](i => step(ACurrentMultipart(List[BucketFilePartUploadTicket](), 0))(i)))
  }

  // main function for iteratee usage
  private def uploadsChunks(fileName: String, contentType: String, bucket: Bucket, acl:Option[ACL]): Iteratee[Array[Byte], Either[S3Exception, PointerToBucketFile]] = {

    val keyName = fileName
    Iteratee.flatten {
      bucket.initiateMultipartUpload(BucketFile(fileName, contentType, acl=acl)).map(bfut => {
        Enumeratee.grouped[Array[Byte]](qualifyChunks) &>> iterateeUploading(bfut, bucket, keyName, contentType, acl).map(iterateeResult => iterateeResult match {
          case AnError(s3error) => Left(s3error)
          case AdirectPutObject(po, length) =>
            Right(PointerToBucketFile(po, contentType, length))
          case ACurrentMultipart(parts, length) => {
            val r = bucket.completeMultipartUpload(bfut, parts)
              .recover({
                case S3Exception(status, code, message, originalXml) => (Left(S3Exception(status, code, message, originalXml)))
              })
              .map(
                unit => (Right(PointerToBucketFile(keyName, contentType, length))))
            Await.result(r, 5 minutes)
          }
        })
      })
    }
  }

  // utils
  private def normalizeString(st: String) = {
    val ascii = Normalizer.normalize(st, Normalizer.Form.NFKD) filter (_ < 128)
    ascii.toLowerCase map { c => if(c.isLetterOrDigit) c else '-' }
  }
  private def goForIt(rh: RequestHeader): Either[SimpleResult, Unit] = Right(Unit)
  private def defaultErrorHandler(s3ex: S3Exception) = InternalServerError
  private def defaultFileNamer(fi: FileInfo) = UUID.randomUUID.toString + "/" + normalizeString(fi.fileName)

  // public API
  def apply(bucket: Bucket, namer: (RequestHeader => String), uploadRigthsChecker: (RequestHeader => Either[SimpleResult, Unit]) = goForIt, s3ErrorHandler: (S3Exception => SimpleResult) = defaultErrorHandler, acl:Option[ACL] = None): BodyParser[PointerToBucketFile] = {
    BodyParser(rh =>
      uploadRigthsChecker(rh) match {
        case Left(sr) => Done(Left(sr))
        case Right(_) => {
          uploadsChunks(namer(rh), rh.contentType.getOrElse("application/octet-stream"), bucket, acl).mapDone(r => {
            val rr: Either[SimpleResult, PointerToBucketFile] = r match {
              case Right(r) => Right(r)
              case Left(s3ex) => Left(s3ErrorHandler(s3ex))
            }
            rr
          })
        }
      })
  }

  def s3MultipartFormBodyParser(bucket: Bucket, namer: (FileInfo => String) = defaultFileNamer, uploadRigthsChecker: (RequestHeader => Either[SimpleResult, Unit]) = goForIt, acl:Option[ACL] = None) = parse.using {
    rh: RequestHeader =>
      uploadRigthsChecker(rh) match {
        case Left(sr) => BodyParser(rh => Done(Left(sr)))
        case Right(_) => parse.multipartFormData(parse.Multipart.handleFilePart(fi => {
          uploadsChunks(namer(fi), fi.contentType.getOrElse("application/octet-stream"), bucket, acl)
        }))
      }
  }

}




