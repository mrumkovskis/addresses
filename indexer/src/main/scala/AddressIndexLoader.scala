package lv.addresses.indexer

import com.typesafe.scalalogging.Logger

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.BufferedWriter
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.io.PrintWriter
import scala.util.Using
import scala.collection.mutable.{ArrayBuffer => AB}
import lv.addresses.index.Index._
import org.slf4j.LoggerFactory

class AddressIndexLoader(indexFiles: IndexFiles) {
  private val logger = Logger(LoggerFactory.getLogger("lv.addresses.indexer"))

  def save(addresses: Addresses,
           idxCode: scala.collection.mutable.HashMap[Int, Int],
           index: MutableIndex) = {

    val IndexFiles(addrFile, idxFile) = indexFiles
    val Addresses(addressMap, history) = addresses

    logger.info(s"Saving addresses $addrFile...")
    Using(new PrintWriter(new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(addrFile), "UTF-8")))) { w =>
      addressMap.foreach { case (_, a) =>
        import a._
        w.println(s"$code;$typ;$name;$superCode;$isLeaf;${
          Option(zipCode).getOrElse("")};${
          Option(coordLat).getOrElse("")};${
          Option(coordLong).getOrElse("")};${
          Option(atvk).getOrElse("")}")
      }
      w.println()
      history.foreach { case (c, h) =>
        w.println(h.mkString(c.toString + ";", ";", ""))
      }
    }.failed.foreach(throw _)

    logger.info(s"Saving address index in $idxFile...")
    Using(new DataOutputStream(new BufferedOutputStream(new FileOutputStream(idxFile)))) { os =>
      var maxRefArrayWord: String = null
      var maxRefArrayLength = 0
      //write address count
      os.writeInt(idxCode.size)
      //write addr_idx->addr_code map
      idxCode foreach { case (i, c) =>
        os.writeInt(i)
        os.writeInt(c)
      }
      index.write { (path: Vector[Int], word: String, refs: Refs) =>
        os.writeInt(path.size)
        path.foreach(os.writeInt)
        os.writeUTF(word)
        def writeRefs(r: AB[Int]) = {
          os.writeInt(r.size)
          r foreach os.writeInt
        }
        //write exact refs
        writeRefs(refs.exact)
        writeRefs(refs.approx)
        //write approx refs
        if (refs.exact.size + refs.approx.size > maxRefArrayLength) {
          maxRefArrayWord = word
          maxRefArrayLength = refs.exact.size + refs.approx.size
        }
      }
      logger.info(s"Max. reference array length for the word '$maxRefArrayWord': $maxRefArrayLength")
    }.failed.foreach(throw _)
    logger.info(s"Address index saved.")
  }

  case class CachedIndex(addresses: Addresses,
                         idxCode: scala.collection.mutable.HashMap[Int, Int],
                         index: MutableIndex)

  def load(): CachedIndex = {
    val IndexFiles(addrFile, idxFile) = indexFiles
    logger.info(s"Loading address index from $idxFile...")
    Using(new DataInputStream(new BufferedInputStream(new FileInputStream(idxFile)))) { in =>
      logger.info(s"Loading address data from $addrFile...")
      var addressMap = Map[Int, AddrObj]()
      var history = Map[Int, List[String]]()
      var lnr = 0
      var hc = 0
      Using(scala.io.Source.fromInputStream(new BufferedInputStream(new FileInputStream(addrFile)), "UTF-8")) {
        _.getLines()
          .foldLeft(false) { (isHistory, l) =>
            if (l.isEmpty) true
            else if (isHistory) {
              lnr += 1
              val a = l.split(";")
              try {
                history += (a.head.toInt -> a.tail.toList)
              } catch {
                case e: Exception => throw new RuntimeException(s"Error at line $lnr: $l", e)
              }
              hc += a.length - 1
              true
            } else {
              lnr += 1
              val a = l.split(";").padTo(9, null)
              val o =
                try AddrObj(a(0).toInt, a(1).toInt, a(2), a(3).toInt, a(5),
                  normalize(a(2)).toVector,
                  Option(a(6)).filter(_.nonEmpty).map(BigDecimal(_)).orNull,
                  Option(a(7)).filter(_.nonEmpty).map(BigDecimal(_)).orNull,
                  Option(a(8)).filter(_.nonEmpty).orNull, a(4).toBoolean)
                catch {
                  case e: Exception => throw new RuntimeException(s"Error at line $lnr: $l", e)
                }
              addressMap += (o.code -> o)
              false
            }
          }
      }.failed.foreach(throw _)

      val idx_code = scala.collection.mutable.HashMap[Int, Int]()
      val index = new MutableIndex(null, null)
      //load addr_idx->addr_code map
      val count = in.readInt
      var i = 0
      while (i < count) {
        idx_code += (in.readInt -> in.readInt)
        i += 1
      }
      //load index
      while (in.available > 0) {
        val pathSize = in.readInt
        val path = new AB[Int](pathSize)
        1 to pathSize foreach (_ => path += in.readInt)
        val word = in.readUTF
        def readRefs = {
          val refsSize = in.readInt
          val refs = new AB[Int](refsSize)
          1 to refsSize foreach (_ => refs += in.readInt)
          refs
        }
        index.load(path.toVector, word, Refs(exact = readRefs, approx = readRefs))
      }

      logger.info(s"Address index loaded (addresses - ${addressMap.size}, historical addresses - $hc), " +
        s"index stats - ${index.statistics.render}.")
      CachedIndex(Addresses(addressMap, history), idx_code, index)
    }.get
  }
}
