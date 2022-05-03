package lv.addresses.indexer

import java.io.{File, InputStreamReader}
import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.util.Properties
import scala.collection.mutable.{ArrayBuffer => AB, Set => MS}
import scala.jdk.CollectionConverters.CollectionHasAsScala

class MutableAddress(var code: Int, var typ: Int, var address: String = null,
                     var zipCode: String = null,
                     var lksCoordLat: BigDecimal = null, var lksCoordLong: BigDecimal = null,
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

case class AddrObj(code: Int, typ: Int, name: String, superCode: Int, zipCode: String,
                   words: Vector[String], coordLat: BigDecimal = null, coordLong: BigDecimal = null,
                   atvk: String = null, isLeaf: Boolean = true) {
  def foldLeft[A](addressMap: Map[Int, AddrObj])(z: A)(o: (A, AddrObj) => A): A =
    addressMap.get(superCode).map(ao =>
      ao.foldLeft(addressMap)(o(z, this))(o)).getOrElse(o(z, this))
  def foldRight[A](addressMap: Map[Int, AddrObj])(z: A)(o: (A, AddrObj) => A): A =
    addressMap.get(superCode).map(ao =>
      ao.foldRight(addressMap)(z)(o)).map(o(_, this)).getOrElse(o(z, this))
  def depth(addressMap: Map[Int, AddrObj]) = foldLeft(addressMap)(0)((d, _) => d + 1)
}

case class Addresses(addresses: Map[Int, AddrObj], history: Map[Int, List[String]])

case class IndexFiles(addresses: File, index: File) {
  def exists: Boolean = addresses.exists() && index.exists()
}

object Constants {
  val PIL = 104
  val NOV = 113
  val PAG = 105
  val CIE = 106
  val IEL = 107
  val NLT = 108
  val DZI = 109

  val writtenOrder = Map (
    IEL -> 0,
    NLT -> 1,
    DZI -> 2,
    CIE -> 3,
    PIL -> 4,
    PAG -> 5,
    NOV -> 6
  )
  val typeOrderMap = Map[Int, Int](
    NOV -> 1, //novads
    PIL -> 2, //pilsēta
    PAG -> 3, //pagasts
    CIE -> 4, //ciems
    IEL -> 5, //iela
    NLT -> 6, //nekustama lieta (māja)
    DZI -> 7 //dzivoklis
  )
  val big_unit_types = Set(PIL, NOV, PAG, CIE)

  val SEPARATOR_REGEXP = """[\s-,/\."'\n]"""

  val WordLengthEditDistances = Map[Int, Int](
    0 -> 0,
    1 -> 0,
    2 -> 0,
    3 -> 1,
    4 -> 1,
    5 -> 1
  )
  val DefaultEditDistance = 2
}

trait AddressFinder
  extends AddressIndexer
    with AddressResolver
    with SpatialIndexer {

  import Constants._
  import AddressFields._

  def addressLoaderFun: () => Addresses
  def indexFiles: IndexFiles

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

    if (indexFiles == null)
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
      if (indexFiles.exists) {
        val cachedIndex = new AddressIndexLoader(indexFiles).load()
        _addressMap = cachedIndex.addresses.addresses
        _addressHistory = cachedIndex.addresses.history
        _index = Index(cachedIndex.idxCode, cachedIndex.index)
      } else {
        val Addresses(am, ah) = addressLoaderFun()
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

  def saveIndex = {
    checkIndex
    new AddressIndexLoader(indexFiles)
      .save(Addresses(addressMap, addressHistory), index.idxCode, index.index)
  }

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
      .sortBy(_.editDistance)
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

  def searchNearest(coordLat: BigDecimal, coordLong: BigDecimal)(limit: Int = 1,
                                                                 fields: Set[String] =
                                                                   Set(
                                                                     StructData,
                                                                     LksKoordData,
                                                                     HistoryData
                                                                   )): Array[MutableAddress] =
    new Search(Math.min(limit, 20))
      .searchNearest(coordLat, coordLong)
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
      if (ma.lksCoordLat == null) ma.lksCoordLat = ao.coordLat
      if (ma.lksCoordLong == null) ma.lksCoordLong = ao.coordLong
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
        ma.lksCoordLat = null
        ma.lksCoordLong = null
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
