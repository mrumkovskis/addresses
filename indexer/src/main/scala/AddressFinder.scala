package lv.addresses.indexer

import java.io.File
import java.sql.DriverManager
import java.time.LocalDateTime

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import org.tresql.{LogTopic, Query, Resources, SimpleCache}

import scala.collection.mutable.{ArrayBuffer => AB, Set => MS}
import scala.util.Using

case class Address(code: Int, address: String, zipCode: String, typ: Int,
                   coordX: BigDecimal, coordY: BigDecimal, history: List[String],
                   editDistance: Option[Int])
case class AddressStruct(
                          pilCode: Option[Int] = None, pilName: Option[String] = None,
                          novCode: Option[Int] = None, novName: Option[String] = None,
                          pagCode: Option[Int] = None, pagName: Option[String] = None,
                          cieCode: Option[Int] = None, cieName: Option[String] = None,
                          ielCode: Option[Int] = None, ielName: Option[String] = None,
                          nltCode: Option[Int] = None, nltName: Option[String] = None,
                          dzvCode: Option[Int] = None, dzvName: Option[String] = None)
case class ResolvedAddress(address: String, resolvedAddress: Option[Address])

trait AddressFinder
  extends AddressIndexer
    with AddressResolver
    with AddressIndexLoader
    with AddressLoader
    with AddressIndexerConfig
    with SpatialIndexer {

  import Constants._

  protected val logger = Logger(LoggerFactory.getLogger("lv.addresses.indexer"))


  private[this] var _addressMap: Map[Int, AddrObj] = null
  private[this] var _addressHistory: Map[Int, List[String]] = Map()

  def addressMap = _addressMap
  def addressHistory = _addressHistory

  def init: Unit = {
    if (ready) return

    if (addressFileName == null && (dbConfig.isEmpty || dbDataVersion == null))
      logger.error("Address file not set nor database connection parameters specified")
    else {
      if (indexFiles.isDefined) loadIndex else {
        val (am, ah) = dbConfig
          .map(loadAddressesFromDb) //load from db
          .getOrElse(loadAddresses() -> Map[Int, List[String]]()) //load from file
        _addressMap = am
        _addressHistory = ah
        index(am, ah)
        saveIndex
      }
      spatialIndex(addressMap)
    }
  }

  def ready = _index != null && addressMap != null

  def checkIndex = if (_index == null || addressMap == null)
    sys.error("""
           Address index not found. Check whether 'VZD.ak-file' property is set and points to existing file.
           If method is called from console make sure that method index(<address register zip file>) or loadIndex is called first""")

  def search(str: String)(limit: Int = 20, types: Set[Int] = null): Array[Address] = {
    checkIndex
    if (str.trim.length == 0) //search string empty, search only big units - PIL, NOV, PAG, CIE
      if (types == null || (types -- big_unit_types) != Set.empty)
        Array[Address]()
      else {
        val result = new AB[Int]()
        var (s, i) = (0, 0)
        while (i < _sortedPilsNovPagCiem.size && s < limit) {
          if(types contains _addressMap(_sortedPilsNovPagCiem(i)).typ) {
            result += _sortedPilsNovPagCiem(i)
            s += 1
          }
          i += 1
        }
        result.map(address(_)).toArray
      }
    else {
      val words = normalize(str)
      def codesToAddr(codes: AB[Int], editDistance: Int, existingCodes: MS[Int]) = {
        var (perfectRankCount, i) = (0, 0)
        val size = Math.min(codes.length, limit)
        val result = new AB[Long](size)
        while (perfectRankCount < size && i < codes.length) {
          val code = codes(i)
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
          .map(address(_, editDistance))
      }
      val resultCodes = searchCodes(words)(1024, types)
      val length = resultCodes.length
      val addresses = new AB[Address]()
      var i = 0
      val existingCodes = MS[Int]()
      while (i < length && addresses.length < limit) {
        val fuzzyRes = resultCodes(i)
        addresses ++= codesToAddr(fuzzyRes.refs, fuzzyRes.editDistance, existingCodes)
        i += 1
      }
      val size = Math.min(limit, addresses.length)
      val result = new Array[Address](size)
      i = 0
      while (i < size) {
        result(i) = addresses(i)
        i += 1
      }
      result
    }
  }

  def searchNearest(coordX: BigDecimal, coordY: BigDecimal)(limit: Int = 1) =
    new Search(Math.min(limit, 20))
      .searchNearest(coordX, coordY)
      .map(result => addressFromObj(result._1))
      .toArray

  def addressStruct(code: Int) = {
    def s(st: AddressStruct, typ: Int, code: Int, name: String) = typ match {
      case PIL => st.copy(pilCode = Option(code), pilName = Option(name))
      case NOV => st.copy(novCode = Option(code), novName = Option(name))
      case PAG => st.copy(pagCode = Option(code), pagName = Option(name))
      case CIE => st.copy(cieCode = Option(code), cieName = Option(name))
      case IEL => st.copy(ielCode = Option(code), ielName = Option(name))
      case NLT => st.copy(nltCode = Option(code), nltName = Option(name))
      case DZI => st.copy(dzvCode = Option(code), dzvName = Option(name))
      case _ => st
    }
    addressMap
      .get(code)
      .map(_.foldLeft(AddressStruct())((st, o) => s(st, o.typ, o.code, o.name)))
      .getOrElse(AddressStruct())
  }

  def getAddressSource(types: Option[Set[Int]]) : Source[Address, NotUsed] = {
    Source(addressMap)
      .map(_._2)
      .filter(a => types.isEmpty || types.get.contains(a.typ))
      .map(addressFromObj(_))
  }

  def addressOption(code: Int, editDistance: Int = 0) =
    addressMap.get(code).map(addressFromObj(_, editDistance))
  private def addressFromObj(addrObj: AddrObj, editDistance: Int = 0): Address = addrObj.foldLeft((
    Map[Int, AddrObj](),
    null: String, //zip code
    0, //address object type
    null: BigDecimal, //coordX
    null: BigDecimal //coordY
  )) { (m, a) =>
    (m._1 + (a.typ -> a),
      if (m._2 == null) a.zipCode else m._2,
      if (m._3 == 0) a.typ else m._3,
      if (m._4 == null) a.coordX else m._4,
      if (m._5 == null) a.coordY else m._5)
  } match { case (ac, zip, typ, coordX, coordY) =>
    val as = new scala.collection.mutable.StringBuilder()
    ac.get(IEL).foreach(iela => as ++= iela.name)
    ac.get(NLT).foreach(maja => as ++= ((if (as.isEmpty) "" else " ") + maja.name))
    ac.get(DZI).foreach(dzivoklis => as ++= (" - " + dzivoklis.name))
    ac.get(CIE).foreach(ciems => as ++= ((if (as.isEmpty) "" else "\n") + ciems.name))
    ac.get(PIL).foreach(pilseta => as ++= ((if (as.isEmpty) "" else "\n") + pilseta.name))
    ac.get(PAG).foreach(pagasts => as ++= ((if (as.isEmpty) "" else "\n") + pagasts.name))
    ac.get(NOV).foreach(novads => as ++= ((if (as.isEmpty) "" else "\n") + novads.name))
    Address(addrObj.code, as.toString, zip, typ, coordX, coordY,
      addressHistory.getOrElse(addrObj.code, Nil), Option(editDistance).filter(_ > 0))
  }
  def address(code: Int, editDistance: Int = 0): Address = addressOption(code, editDistance).get

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

  def objsInWrittenOrder(addrObj: AddrObj) = addrObj.foldLeft(new Array[AddrObj](7)) { (a, o) =>
    a(writtenOrder(o.typ)) = o
    a
  }.filter(_ != null)

  def saveIndex = {
    checkIndex
    save(addressMap, _idxCode, _index, _sortedPilsNovPagCiem)
  }

  def loadIndex = {
    val idx = load()
    _addressMap = idx.addresses
    _idxCode = idx.idxCode
    _index = idx.index
    _sortedPilsNovPagCiem = idx.sortedBigObjs
    _addressHistory = idx.history
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
