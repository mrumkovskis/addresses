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

trait AddressIndexLoader { this: AddressFinder =>
  def save(addressMap: Map[Int, AddrObj],
           idxCode: scala.collection.mutable.HashMap[Int, Int],
           index: scala.collection.mutable.Map[String, Array[Int]],
           sortedPilNovPagCiem: Vector[Int]) = {

    val IndexFiles(idxFile, addrFile) = newIndexFiles
    logger.info(s"Saving address index in $idxFile...")
    val maxRefArray = index.maxBy(_._2.length)
    val maxRefArrayLength = maxRefArray._2.length
    logger.info(s"Max. reference array length for the word '${maxRefArray._1}': ${maxRefArray._2.length}")
    val os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(idxFile)))
    try {
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
      os.writeInt(maxRefArrayLength)
      //write word->addr_indexes map
      index.foreach(i => {
        os.writeUTF(i._1)
        os.writeInt(i._2.length)
        i._2 foreach os.writeInt
      })
    } finally os.close

    logger.info(s"Saving addresses $addrFile...")
    val w = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(addrFile), "UTF-8")))
    try {
      addressMap.foreach(a => {
        import a._2._
        w.println(s"$code;$typ;$name;$superCode;${
          Option(zipCode).getOrElse("")};${
          Option(coordX).getOrElse("")};${
          Option(coordY).getOrElse("")}")
      })
    } finally w.close
    logger.info(s"Address index saved.")
  }

  case class Index(addresses: Map[Int, AddrObj],
                   idxCode: scala.collection.mutable.HashMap[Int, Int],
                   index: scala.collection.mutable.HashMap[String, Array[Int]],
                   sortedBigObjs: Vector[Int],
                   history: Map[Int, List[String]])

  def load(): Index = {
    val IndexFiles(idxFile, addrFile) = indexFiles.getOrElse(sys.error(s"Index files not found"))
    logger.info(s"Loading address index from $idxFile...")
    val in = new DataInputStream(new BufferedInputStream(new FileInputStream(idxFile)))
    val idx_code = scala.collection.mutable.HashMap[Int, Int]()
    val index = scala.collection.mutable.HashMap[String, Array[Int]]()
    var c = 0
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
    var a = new Array[Int](in.readInt)
    try {
      while (in.available > 0) {
        i = 0
        val w = in.readUTF
        val l = in.readInt
        if (a.length < l) a = new Array[Int](l)
        //(0 until l) foreach (a(_) = in.readLong) this is slow
        i = 0
        while (i < l) {
          a(i) = in.readInt
          i += 1
        }
        index += (w -> (a take l))
        c += 1
      }
    } finally in.close
    logger.info(s"Total words loaded: $c")
    logger.info(s"Loading address data from $addrFile...")
    var addressMap = Map[Int, AddrObj]()
    var ac = 0
    scala.io.Source.fromInputStream(new BufferedInputStream(new FileInputStream(addrFile)), "UTF-8")
      .getLines()
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

    val history = dbConfig.map { case conf @ DbConfig(driver, url, user, pwd, _) =>
      Class.forName(driver)
      val conn = DriverManager.getConnection(url, user, pwd)
      try {
        loadAddressHistoryFromDb(conn)(conf.tresqlResources)
      } finally conn.close()
    }.getOrElse(Map())

    index.foreach { case (w, c) => c.foreach(_index.updateChildren(w, _)) }
    logger.info(s"Address index loaded (addresses - $ac, historical addresses - ${history.size}), " +
      s"index stats - ${_index.statistics.render}.")
    Index(addressMap, idx_code, index, spnpc.toVector, history)
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
