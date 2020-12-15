package lv.addresses.indexer

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.BufferedWriter
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.io.PrintWriter
import java.sql.DriverManager

import scala.util.Using
import scala.collection.mutable.{ArrayBuffer => AB}
import lv.addresses.index.Index._

trait AddressIndexLoader { this: AddressFinder =>
  def save(addressMap: Map[Int, AddrObj],
           idxCode: scala.collection.mutable.HashMap[Int, Int],
           index: MutableIndex,
           sortedPilNovPagCiem: Vector[Int]) = {

    val IndexFiles(addrFile, idxFile) = newIndexFiles

    logger.info(s"Saving addresses $addrFile...")
    Using(new PrintWriter(new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(addrFile), "UTF-8")))) { w =>
      addressMap.foreach(a => {
        import a._2._
        w.println(s"$code;$typ;$name;$superCode;${
          Option(zipCode).getOrElse("")};${
          Option(coordX).getOrElse("")};${
          Option(coordY).getOrElse("")}")
      })
    }

    logger.info(s"Saving address index in $idxFile...")
    Using(new DataOutputStream(new BufferedOutputStream(new FileOutputStream(idxFile)))) { os =>
      var maxRefArrayWord: String = null
      var maxRefArrayLength = 0
      //write big object count
      os.writeInt(sortedPilNovPagCiem.size)
      //write big objects
      sortedPilNovPagCiem foreach os.writeInt
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
    }
    logger.info(s"Address index saved.")
  }

  case class Index(addresses: Map[Int, AddrObj],
                   idxCode: scala.collection.mutable.HashMap[Int, Int],
                   index: MutableIndex,
                   sortedBigObjs: Vector[Int],
                   history: Map[Int, List[String]])

  def load(): Index = {
    val IndexFiles(addrFile, idxFile) = indexFiles.getOrElse(sys.error(s"Index files not found"))
    logger.info(s"Loading address index from $idxFile...")
    Using(new DataInputStream(new BufferedInputStream(new FileInputStream(idxFile)))) { in =>
      logger.info(s"Loading address data from $addrFile...")
      var addressMap = Map[Int, AddrObj]()
      var ac = 0
      Using(scala.io.Source.fromInputStream(new BufferedInputStream(new FileInputStream(addrFile)), "UTF-8")) {
        _.getLines()
          .foreach { l =>
            ac += 1
            val a = l.split(";").padTo(7, null)
            val o =
              try AddrObj(a(0).toInt, a(1).toInt, a(2), a(3).toInt, a(4),
                normalize(a(2)).toVector,
                Option(a(5)).filter(_.length > 0).map(BigDecimal(_)).orNull,
                Option(a(6)).filter(_.length > 0).map(BigDecimal(_)).orNull)
              catch {
                case e: Exception => throw new RuntimeException(s"Error at line $ac: $l", e)
              }
            addressMap += (o.code -> o)
          }
      }

      val history = dbConfig.map { case conf @ DbConfig(driver, url, user, pwd, _) =>
        Class.forName(driver)
        Using(DriverManager.getConnection(url, user, pwd)) { conn =>
          loadAddressHistoryFromDb(conn)(conf.tresqlResources)
        }.get
      }.getOrElse(Map())

      val idx_code = scala.collection.mutable.HashMap[Int, Int]()
      val index = new MutableIndex(null, null)
      val spnpc = new Array[Int](in.readInt)
      //load pilseta, novads, pagasts, ciems
      var i = 0
      while(i < spnpc.length) {
        spnpc(i) = in.readInt
        i += 1
      }
      //load addr_idx->addr_code map
      val count = in.readInt
      i = 0
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

      logger.info(s"Address index loaded (addresses - $ac, historical addresses - ${history.size}), " +
        s"index stats - ${index.statistics.render}.")
      Index(addressMap, idx_code, index, spnpc.toVector, history)
    }.get
  }

  private def indexFilesInternal: Option[IndexFiles] =
    Option(dbDataVersion)
      .flatMap(v => dbConfig.map(v -> _.indexDir))
      .map { case (ver, indexDir) =>
        val dir = new File(indexDir)
        if (!dir.isDirectory) sys.error(s"$dir not a directory, cannot store index file." +
          s"Please check that setting 'db.index-dir' points to existing directory")
        IndexFiles(addresses = new File(dir, s"$DbDataFilePrefix$ver.$AddressesPostfix"),
          index = new File(dir, s"$DbDataFilePrefix$ver.$IndexPostfix"))
      }
      .orElse {
        Option(addressFileName).map { fn =>
          val akFile = new File(fn)
          val filePrefix = akFile.getName.substring(0, akFile.getName.lastIndexOf("."))
          IndexFiles(addresses = new File(akFile.getParent, filePrefix + s".$AddressesPostfix"),
            index = new File(akFile.getParent, filePrefix + s".$IndexPostfix"))
        }
      }

  def indexFiles: Option[IndexFiles] =
    indexFilesInternal.filter(i => i.addresses.exists() && i.index.exists())

  def newIndexFiles: IndexFiles = {
    indexFilesInternal.map { case i @ IndexFiles(addresses, index) =>
      if (addresses.exists()) sys.error(s"Cannot save address file. File $addresses already exists")
      if (index.exists()) sys.error(s"Cannot save address index file. File $index already exists")
      i
    }.getOrElse(sys.error(s"Cannot create index files because no address data found neither in database nor file."))
  }
}
