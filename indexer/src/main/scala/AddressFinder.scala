package lv.addresses.indexer

import java.io.{File, InputStreamReader}
import java.sql.DriverManager
import java.time.LocalDateTime
import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import org.tresql.{LogTopic, Query, Resources, SimpleCache}

import java.util.Properties
import scala.collection.mutable.{ArrayBuffer => AB, Set => MS}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Using

class MutableAddress(var code: Int, var typ: Int, var address: String = null,
                     var zipCode: String = null,
                     var lksCoordX: BigDecimal = null, var lksCoordY: BigDecimal = null,
                     var history: List[String] = null,
                     var editDistance: Option[Int] = None,
                     var pilCode: Option[Int] = None, var pilName: Option[String] = None,
                     var novCode: Option[Int] = None, var novName: Option[String] = None,
                     var pagCode: Option[Int] = None, var pagName: Option[String] = None,
                     var cieCode: Option[Int] = None, var cieName: Option[String] = None,
                     var ielCode: Option[Int] = None, var ielName: Option[String] = None,
                     var nltCode: Option[Int] = None, var nltName: Option[String] = None,
                     var dzvCode: Option[Int] = None, var dzvName: Option[String] = None,
                     var pilAtvk: Option[String] = None,
                     var novAtvk: Option[String] = None,
                     var pagAtvk: Option[String] = None,
                     var irAdrese: Boolean = true
                    )

case class ResolvedAddress(address: String, resolvedAddress: Option[MutableAddress])

object AddressFields {
  val StructData = "struct"
  val LksKoordData = "lks_koord"
  val HistoryData = "history"
  val AtvkData = "atvk"
}

trait AddressFinder
  extends AddressIndexer
    with AddressResolver
    with AddressIndexLoader
    with AddressLoader
    with AddressIndexerConfig
    with SpatialIndexer {

  import Constants._
  import AddressFields._

  protected val logger = Logger(LoggerFactory.getLogger("lv.addresses.indexer"))


  private[this] var _addressMap: Map[Int, AddrObj] = null
  private[this] var _addressHistory: Map[Int, List[String]] = Map()

  def addressMap = _addressMap
  def addressHistory = _addressHistory

  private[this] var _index: Index = null
  private[this] var _bigObjIndex: Index = null

  def index = _index
  def bigObjIndex = _bigObjIndex

  def init: Unit = {
    if (ready) return

    if (addressFileName == null && (dbConfig.isEmpty || dbDataVersion == null))
      logger.error("Address file not set nor database connection parameters specified")
    else {
      logger.info("Loading address synonyms...")
      val synonyms: Properties = {
        val props = new Properties()
        val in = getClass.getResourceAsStream("/synonyms.properties")
        if (in != null) props.load(new InputStreamReader(in, "UTF-8"))
        //remove diacritics from keys
        props.stringPropertyNames.asScala.foreach { key =>
          val nk = lv.addresses.index.Index.normalize(key).mkString("")
          if (nk != key) {
            val v = props.getProperty(key)
            props.remove(key)
            props.setProperty(nk, v)
          }
        }
        props
      }
      logger.debug(s"Synonyms: $synonyms")
      logger.info(s"${synonyms.size} address synonym(s) loaded")
      if (indexFiles.isDefined) {
        val cachedIndex = load()
        _addressMap = cachedIndex.addresses
        _addressHistory = cachedIndex.history
        _index = Index(cachedIndex.idxCode, cachedIndex.index)
      } else {
        val (am, ah) = dbConfig
          .map(loadAddressesFromDb) //load from db
          .getOrElse(loadAddresses() -> Map[Int, List[String]]()) //load from file
        _addressMap = am
        _addressHistory = ah
        _index = index(am, ah, synonyms, null)
        saveIndex
      }
      logger.info("Creating big object index...")
      _bigObjIndex = index(addressMap, addressHistory, synonyms, ao => big_unit_types(ao.typ))
      logger.info("Creating big object index done")
      spatialIndex(addressMap)
    }
  }

  def ready = _index != null && addressMap != null

  def checkIndex = if (_index == null || addressMap == null)
    sys.error("""
           Address index not found. Check whether 'VZD.ak-file' property is set and points to existing file or
           VZD.{driver, user, password} and db properties are set.
           If method is called from console make sure that method index(<address register zip file>) or loadIndex is called first""")

  def searchIndex(str: String,
                  limit: Int,
                  types: Set[Int],
                  fields: Set[String],
                  idx: Index): Array[MutableAddress] = {
    import lv.addresses.index.Index._
    checkIndex
    val words = normalize(str)
    def codesToAddr(refs: AB[Int], editDistance: Int, existingCodes: MS[Int]) = {
      var (perfectRankCount, i) = (0, 0)
      val size = Math.min(refs.length, limit)
      val result = new AB[Long](size)
      while (perfectRankCount < size && i < refs.length) {
        val code = idx.idxCode(refs(i))
        if (!existingCodes.contains(code)) {
          val r = rank(words, code)
          if (r == 0) perfectRankCount += 1
          val key = r.toLong << 53 | i.toLong << 32 | code
          insertIntoHeap(result, key)
          existingCodes += code
        }
        i += 1
      }
      val res =
        if (size < result.size / 2) heap_topx(result, size)
        else if (size < result.size) result.sorted.take(size) else result.sorted
      res
        .map(_ & 0x00000000FFFFFFFFL)
        .map(_.toInt)
        .map(mutableAddress(_, fields, editDistance))
    }
    val resultCodes = searchCodes(words, idx.index, maxEditDistance)(1024,
      Option(types).map(t => (i: Int) => t(addressMap(idx.idxCode(i)).typ)).orNull)
    val length = resultCodes.length
    val addresses = new AB[MutableAddress]()
    var i = 0
    val existingCodes = MS[Int]()
    while (i < length && addresses.length < limit) {
      val fuzzyRes = resultCodes(i)
      addresses ++= codesToAddr(fuzzyRes.refs, fuzzyRes.editDistance, existingCodes)
      i += 1
    }
    val size = Math.min(limit, addresses.length)
    val result = new Array[MutableAddress](size)
    i = 0
    while (i < size) {
      result(i) = addresses(i)
      i += 1
    }
    result
  }

  def search(str: String)(limit: Int = 20,
                          types: Set[Int] = null,
                          fields: Set[String] =
                            Set(StructData, LksKoordData, HistoryData)): Array[MutableAddress] = {
    if (str == null || str.trim.isEmpty) {
      Array()
    } else {
      searchIndex(str, limit, types, fields, if (types == null || types.isEmpty) index else bigObjIndex)
    }
  }

  def searchNearest(coordX: BigDecimal, coordY: BigDecimal)(limit: Int = 1,
                                                            fields: Set[String] =
                                                              Set(StructData,
                                                                LksKoordData,
                                                                HistoryData)): Array[MutableAddress] =
    new Search(Math.min(limit, 20))
      .searchNearest(coordX, coordY)
      .map(result => mutableAddressFromObj(result._1, fields))
      .toArray

  def getAddressSource(types: Option[Set[Int]]) : Source[MutableAddress, NotUsed] = {
    Source(addressMap)
      .map(_._2)
      .filter(a => types.isEmpty || types.get.contains(a.typ))
      .map(mutableAddressFromObj(_, Set(LksKoordData)))
  }

  def mutableAddressOption(code: Int, fields: Set[String] = Set(StructData, LksKoordData, HistoryData),
                           editDistance: Int = 0): Option[MutableAddress] = {
    addressMap.get(code).map(mutableAddressFromObj(_, fields, editDistance))
  }

  def mutableAddress(code: Int, fields: Set[String], editDistance: Int = 0): MutableAddress = {
    mutableAddressFromObj(addressMap(code), fields, editDistance)
  }

  private def mutableAddressFromObj(addrObj: AddrObj, fields: Set[String], editDistance: Int = 0): MutableAddress = {
    addrObj.foldLeft(addressMap)(new MutableAddress(addrObj.code, addrObj.typ)) { (ma, ao) =>
      if (ma.zipCode == null) ma.zipCode = ao.zipCode
      if (ma.lksCoordX == null) ma.lksCoordX = ao.coordX
      if (ma.lksCoordY == null) ma.lksCoordY = ao.coordY
      ao.typ match {
        case PIL =>
          ma.pilCode = Option(ao.code)
          ma.pilName = Option(ao.name)
          ma.pilAtvk = Option(ao.atvk)
        case NOV =>
          ma.novCode = Option(ao.code)
          ma.novName = Option(ao.name)
          ma.novAtvk = Option(ao.atvk)
        case PAG =>
          ma.pagCode = Option(ao.code)
          ma.pagName = Option(ao.name)
          ma.pagAtvk = Option(ao.atvk)
        case CIE =>
          ma.cieCode = Option(ao.code)
          ma.cieName = Option(ao.name)
        case IEL =>
          ma.ielCode = Option(ao.code)
          ma.ielName = Option(ao.name)
        case NLT =>
          ma.nltCode = Option(ao.code)
          ma.nltName = Option(ao.name)
        case DZI =>
          ma.dzvCode = Option(ao.code)
          ma.dzvName = Option(ao.name)
        case _ =>
      }
      ma
    } match { case ma =>
      val as = new scala.collection.mutable.StringBuilder()
      ma.ielName.foreach(iela => as ++= iela)
      ma.nltName.foreach(maja => as ++= ((if (as.isEmpty) "" else " ") + maja))
      ma.dzvName.foreach(dzivoklis => as ++= (" - " + dzivoklis))
      ma.cieName.foreach(ciems => as ++= ((if (as.isEmpty) "" else "\n") + ciems))
      ma.pilName.foreach(pilseta => as ++= ((if (as.isEmpty) "" else "\n") + pilseta))
      ma.pagName.foreach(pagasts => as ++= ((if (as.isEmpty) "" else "\n") + pagasts))
      ma.novName.foreach(novads => as ++= ((if (as.isEmpty) "" else "\n") + novads))
      ma.address = as.toString
      if (fields(HistoryData)) ma.history = addressHistory.getOrElse(ma.code, Nil)
      if (!fields(LksKoordData)) {
        ma.lksCoordX = null
        ma.lksCoordY = null
      }
      //clear unnecessary fields
      if (!fields(StructData)) {
        ma.pilCode = None
        ma.pilName = None
        ma.novCode = None
        ma.novName = None
        ma.pagCode = None
        ma.pagName = None
        ma.cieCode = None
        ma.cieName = None
        ma.ielCode = None
        ma.ielName = None
        ma.nltCode = None
        ma.nltName = None
        ma.dzvCode = None
        ma.dzvName = None
      }
      if (!fields(AtvkData)) {
        ma.pilAtvk = None
        ma.novAtvk = None
        ma.pagAtvk = None
      }
      ma.irAdrese = addrObj.isLeaf
      ma.editDistance = Option(editDistance).filter(_ > 0)
      ma
    }
  }

  /**Integer of which last 10 bits are significant.
   * Of them 5 high order bits denote sequential word match count, 5 low bits denote exact word match count.
   * 0 is highest ranking meaning all words sequentially have exact match */
  def rank(words: AB[String], code: Int) = {
    val wl = Math.min(words.length, 32) //max 32 words can be processed in ranking
    def count(s: Int, n: Vector[String]) = {
      var idx = s >> 16 //idx are 16 oldest bits
      var seqCount: Int = s >> 8 & 0x000000FF // seq match count are 8 oldest bits from 16 youngest bits
      var exactCount: Int = s & 0x000000FF // exact match count are 8 youngest bits
      var j = 0
      val nl = n.length
      while (idx < wl && j < nl) {
        if (n(j).startsWith(words(idx))) {
          if (n(j).length == words(idx).length) exactCount += 1
          seqCount += 1
          idx += 1
        }
        j += 1
      }
      (idx << 16) | (seqCount << 8) | exactCount
    }
    val addrObjs = objsInWrittenOrder(addressMap(code))
    val aol = addrObjs.length
    def run(idx: Int, idxObjs: Int): Int =
      if (idx >> 16 < wl && idxObjs < aol)
        run(count(idx, addrObjs(idxObjs).words), idxObjs + 1)
      else idx
    val r = run(0, 0)
    val a = wl - (r >> 8 & 0x000000FF)
    val b = wl - (r & 0x000000FF)
    a << 5 | b
  }

  def objsInWrittenOrder(addrObj: AddrObj) = addrObj.foldLeft(addressMap)(new Array[AddrObj](7)) { (a, o) =>
    a(writtenOrder(o.typ)) = o
    a
  }.filter(_ != null)

  def saveIndex = {
    checkIndex
    save(addressMap, index.idxCode, index.index)
  }

  def insertIntoHeap(h: AB[Long], el: Long) = {
    var i = h.size
    var j = 0
    h += el
    do {
      j = (i - 1) / 2
      if (h(i) < h(j)) {
        val x = h(i)
        h(i) = h(j)
        h(j) = x
      }
      i = j
    } while (j > 0)
  }
  def heap_topx(h: AB[Long], x: Int) = {
    def swap(i: Int, j: Int) = {
      val x = h(j)
      h(j) = h(i)
      h(i) = x
    }
    //sort heap
    var (i, c, n) = (0, x, h.size)
    while (n > 0 && c > 0) {
      n -= 1
      c -= 1
      swap(0, n)
      i = 0
      while (i < n) {
        var j = i * 2 + 1
        var k = j + 1
        if (j < n)
          if (k < n && h(k) < h(j))
            if (h(k) < h(i)) {
              swap(i, k)
              i = k
            } else i = n
          else if (h(j) < h(i)) {
            swap(i, j)
            i = j
          } else i = n
        else i = n
      }
    }
    //take top x elements
    val na = new Array[Long](x)
    i = 0
    val s = h.size - 1
    while (i < x) {
      na(i) = h(s - i)
      i += 1
    }
    AB(scala.collection.immutable.ArraySeq.unsafeWrapArray(na): _*)
  }

  def heapsortx(a: AB[Long], x: Int) = {
    def swap(i: Int, j: Int) = {
      val x = a(j)
      a(j) = a(i)
      a(i) = x
    }
    //build heap
    var (i, n) = (0, a.length)
    while (i < n) {
      var j = i
      do {
        var k = j
        j = (j - 1) / 2
        if (a(j) > a(k)) swap(j, k)
      } while (j > 0)
      i += 1
    }
    //get x smallest elements
    heap_topx(a, x)
  }
}

case class DbConfig(driver: String, url: String, user: String, password: String, indexDir: String) {
  lazy val tresqlResources = new Resources {
    val infoLogger = Logger(LoggerFactory.getLogger("org.tresql"))
    val tresqlLogger = Logger(LoggerFactory.getLogger("org.tresql.tresql"))
    val sqlLogger = Logger(LoggerFactory.getLogger("org.tresql.db.sql"))
    val varsLogger = Logger(LoggerFactory.getLogger("org.tresql.db.vars"))
    val sqlWithParamsLogger = Logger(LoggerFactory.getLogger("org.tresql.sql_with_params"))

    override val logger = (m, params, topic) => topic match {
      case LogTopic.sql => sqlLogger.debug(m)
      case LogTopic.tresql => tresqlLogger.debug(m)
      case LogTopic.params => varsLogger.debug(m)
      case LogTopic.sql_with_params => sqlWithParamsLogger.debug(sqlWithParams(m, params))
      case LogTopic.info => infoLogger.debug(m)
      case _ => infoLogger.debug(m)
    }

    override val cache = new SimpleCache(4096)

    def sqlWithParams(sql: String, params: Map[String, Any]) = params.foldLeft(sql) {
      case (sql, (name, value)) => sql.replace(s"?/*$name*/", value match {
        case _: Int | _: Long | _: Double | _: BigDecimal | _: BigInt | _: Boolean => value.toString
        case _: String | _: java.sql.Date | _: java.sql.Timestamp => s"'$value'"
        case null => "null"
        case _ => value.toString
      })
    }
  }

  def lastSyncTime: LocalDateTime = {
    Class.forName(driver)
    Using(DriverManager.getConnection(url, user, password)) { conn =>
      implicit val res = tresqlResources withConn conn
      Query("(art_vieta { max (sync_synced) d } +" +
        "art_nlieta { max (sync_synced) d } +" +
        "art_dziv { max (sync_synced) d }) { max(d) }").unique[LocalDateTime]
    }.recover {
      case e: Exception =>
        Logger(LoggerFactory.getLogger("org.tresql")).error("Error getting last sync time", e)
        null
    }.get
  }
}

case class IndexFiles(addresses: File, index: File)

trait AddressIndexerConfig {
  protected val DbDataFilePrefix = "VZD_AR_"
  protected val AddressesPostfix = "addresses"
  protected val IndexPostfix = "index"
  def addressFileName: String
  def blackList: Set[String]
  def houseCoordFile: String

  def dbConfig: Option[DbConfig]

  def dbDataVersion: String =
    dbConfig
      .map(_.lastSyncTime)
      .flatMap(Option(_))
      .map(DbDataFilePrefix + _.toString.replace(':', '_').replace('.', '_'))
      .orNull
}
