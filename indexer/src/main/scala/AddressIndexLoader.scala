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
           index: scala.collection.mutable.Map[String, Array[Long]],
           sortedPilNovPagCiem: Vector[Int]) = {

    logger.info(s"Saving address index for $addressFileName...")
    val idxFile = indexFile(addressFileName)
    if (idxFile.exists) sys.error(s"Cannot save address index file. File $idxFile already exists")
    val maxRefArray = index.maxBy(_._2.length)
    val maxRefArrayLength = maxRefArray._2.length
    logger.info(s"Max. reference array length for the word '${maxRefArray._1}': ${maxRefArray._2.length}")
    val os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(idxFile)))
    try {
      os.writeInt(sortedPilNovPagCiem.size)
      sortedPilNovPagCiem foreach os.writeInt
      os.writeInt(maxRefArrayLength)
      index.foreach(i => {
        os.writeUTF(i._1)
        os.writeInt(i._2.length)
        i._2 foreach os.writeLong
      })
    } finally os.close

    val addrFile = addressCacheFile(addressFileName)
    if (addrFile.exists) sys.error(s"Cannot save address file. File $addrFile already exists")
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
                   index: scala.collection.mutable.HashMap[String, Array[Long]],
                   sortedBigObjs: Vector[Int],
                   history: Map[Int, List[String]])

  def load(): Index = {
    logger.info(s"Loading address index for $addressFileName...")
    val idxFile = indexFile(addressFileName)
    if (!idxFile.exists) sys.error(s"Index file $idxFile not found")
    val in = new DataInputStream(new BufferedInputStream(new FileInputStream(idxFile)))
    val index = scala.collection.mutable.HashMap[String, Array[Long]]()
    var c = 0
    val spnpc = new Array[Int](in.readInt)
    //load pilseta, novads, pagasts, ciems
    var i = 0
    while(i < spnpc.length) {
      spnpc(i) = in.readInt
      i += 1
    }
    var a = new Array[Long](in.readInt)
    try {
      while (in.available > 0) {
        i = 0
        val w = in.readUTF
        val l = in.readInt
        if (a.length < l) a = new Array[Long](l)
        //(0 until l) foreach (a(_) = in.readLong) this is slow
        i = 0
        while (i < l) {
          a(i) = in.readLong
          i += 1
        }
        index += (w -> (a take l))
        c += 1
      }
    } finally in.close
    logger.info(s"Total words loaded: $c")
    val addrFile = addressCacheFile(addressFileName)
    if (!addrFile.exists) sys.error(s"Address file $addrFile not found")
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

    val history = dbConfig.map { case DbConfig(driver, url, user, pwd) =>
      Class.forName(driver)
      val conn = DriverManager.getConnection(url, user, pwd)
      try {
        loadAddressHistoryFromDb(conn)
      } finally conn.close()
    }.getOrElse(Map())

    logger.info(s"Address index loaded (words - $c, addresses - $ac, historical addresses - ${history.size}).")
    Index(addressMap, index, spnpc.toVector, history)
  }

  def hasIndex(akFileName: String) = addressCacheFile(akFileName).exists && indexFile(akFileName).exists

  def addressCacheFile(akFileName: String) = cacheFile(akFileName, "addresses")
  def indexFile(akFileName: String) = cacheFile(akFileName, "index")

  private def cacheFile(akFileName: String, extension: String) = {
    val akFile = new File(akFileName)
    val filePrefix = akFile.getName.split("\\.").head
    new File(akFile.getParent, filePrefix + s".$extension")
  }
}
