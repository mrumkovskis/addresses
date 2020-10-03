package lv.addresses.indexer

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.language.postfixOps
import scala.collection.mutable.{ArrayBuffer => AB}

private object Constants {
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
    PAG -> 2, //pagasts
    PIL -> 3, //pilsēta
    CIE -> 4, //ciems
    IEL -> 5, //iela
    NLT -> 6, //nekustama lieta (māja)
    DZI -> 7 //dzivoklis
  )
  val big_unit_types = Set(PIL, NOV, PAG, CIE)

  val SEPARATOR_REGEXP = """[\s-,/\."'\n]"""
}

trait AddressIndexer { this: AddressFinder =>

  import Constants._

  case class AddrObj(code: Int, typ: Int, name: String, superCode: Int, zipCode: String,
      words: Vector[String], coordX: BigDecimal = null, coordY: BigDecimal = null) {
    def foldLeft[A](z: A)(o: (A, AddrObj) => A): A =
      addressMap.get(superCode).map(ao => ao.foldLeft(o(z, this))(o)).getOrElse(o(z, this))
    def foldRight[A](z: A)(o: (A, AddrObj) => A): A =
      addressMap.get(superCode).map(ao => ao.foldRight(z)(o)).map(o(_, this)).getOrElse(o(z, this))
    def depth = foldLeft(0)((d, _) => d + 1)
  }

  protected var _idxCode: scala.collection.mutable.HashMap[Int, Int] = null
  protected var _index: scala.collection.mutable.HashMap[String, Array[Int]] = null
  //filtering without search string, only by object type code support for (pilsēta, novads, pagasts, ciems)
  protected var _sortedPilsNovPagCiem: Vector[Int] = null

  def searchCodes(words: Array[String])(limit: Int, types: Set[Int] = null): Array[Int] = {
    def searchParams(words: Array[String]) = wordStatForSearch(words)
      .map { case (w, c) => if (c == 1) w else s"$c*$w" }.toArray
    def idx_vals(word: String) = _index.getOrElse(word, Array[Int]())
    def has_type(addr_idx: Int) = types(addressMap(_idxCode(addr_idx)).typ)
    def intersect(idx: Array[Array[Int]], limit: Int): Array[Int] = {
      val result = AB[Int]()
      val pos = Array.fill(idx.length)(0)
      def check_register = {
        val v = idx(0)(pos(0))
        val l = pos.length
        var i = 1
        while (i < l && v == idx(i)(pos(i))) i += 1
        if (i == l) {
          if (types == null || has_type(v)) result append v
          i = 0
          while (i < l) {
            pos(i) += 1
            i += 1
          }
        }
      }
      def find_equal(a_pos: Int, b_pos: Int) = {
        val a: Array[Int] = idx(a_pos)
        val b: Array[Int] = idx(b_pos)
        val al = a.length
        val bl = b.length
        var ai = pos(a_pos)
        var bi = pos(b_pos)
        while (ai < al && bi < bl && a(ai) != b(bi)) if (a(ai) < b(bi)) ai += 1 else bi += 1
        pos(a_pos) = ai
        pos(b_pos) = bi
      }
      def continue = {
        var i = 0
        val l = pos.length
        while (i < l && pos(i) < idx(i).length) i += 1
        i == l
      }
      while (result.length < limit && continue) {
        check_register
        var i = 0
        val l = pos.length - 1
        while(i < l) {
          find_equal(i, i + 1)
          i += 1
        }
      }
      result.map(_idxCode(_)).toArray
    }

    (searchParams(words) map idx_vals sortBy(_.size)) match {
      case Array() => Array[Int]()
      case result => intersect(result, limit)
    }
  }

  def index(addressMap: Map[Int, AddrObj], history: Map[Int, List[String]]) = {

    logger.info("Starting address indexing...")
    logger.info(s"Sorting ${addressMap.size} addresses...")

    //(addressCode, ordering weight, full space separated unnaccented address)
    val addresses = new Array[(Int, Int, String)](addressMap.size)
    var idx = 0
    addressMap.foreach { case (code, addr) =>
      addresses(idx) = (code, typeOrderMap(addr.typ) * 100 + addr.depth,
          addr.foldRight(new scala.collection.mutable.StringBuilder())((b, o) =>
            b.append(" ").append(unaccent(o.name))).toString)
      idx += 1
    }
    val sortedAddresses = addresses.sortWith { case ((_, ord1, a1), (_, ord2, a2)) =>
      ord1 < ord2 || (ord1 == ord2 && (a1.length < a2.length || (a1.length == a2.length && a1 < a2)))
    }

    _sortedPilsNovPagCiem =
      sortedAddresses
        .collect { case (a, _, _) if big_unit_types.contains(addressMap(a).typ) => a }
        .toVector
    logger.info(s"Total size of pilseta, novads, pagasts, ciems - ${_sortedPilsNovPagCiem.size}")

    logger.info("Creating index...")
    val idx_code = scala.collection.mutable.HashMap[Int, Int]()
    idx = 0
    val index = sortedAddresses
      .foldLeft(scala.collection.mutable.HashMap[String, AB[Int]]()) {
        case (index, (code, _, name)) =>
          val wordsLists = extractWords(name) ::
            history.getOrElse(code, Nil).map(extractWords)
          val existingWords = scala.collection.mutable.Set[String]()
          wordsLists foreach { words =>
            words foreach { w =>
              if (!existingWords(w)) {
                existingWords += w
                if (index contains w) index(w).append(idx)
                else index(w) = AB(idx)
              }
            }
          }
          idx_code += (idx -> code)
          idx += 1
          if (idx % 5000 == 0) logger.info(s"Addresses processed: $idx; word cache size: ${index.size}")
          index
      }.map(t => t._1 -> t._2.toArray)

    val refCount = index.foldLeft(0L)((c, t) => c + t._2.size)
    logger.info(s"Address objects processed: $idx; word cache size: ${index.size}; ref count: $refCount")

    this._idxCode = idx_code
    this._index = index
  }

  val accents = "ēūīāšģķļžčņ" zip "euiasgklzcn" toMap

  def unaccent(str: String) = str
    .toLowerCase
    .foldLeft(new scala.collection.mutable.StringBuilder(str.length))(
      (b, c) => b.append(accents.getOrElse(c, c)))
    .toString

  def isWhitespaceOrSeparator(c: Char) = c.isWhitespace || "-,/.\"'\n".contains(c)

  //better performance, whitespaces are eliminated in the same run as unaccent operation
  def normalize(str: String) = str
    .toLowerCase
    .foldLeft(AB[scala.collection.mutable.StringBuilder]() -> true){
       case ((s, b), c) =>
         if (isWhitespaceOrSeparator(c)) (s, true) else {
           if (b) s.append(new scala.collection.mutable.StringBuilder)
           s.last.append(accents.getOrElse(c, c))
           (s, false)
         }
    }._1
    .map(_.toString)
    .toArray

  def wordStatForSearch(words: Array[String]) =
    ((words groupBy identity map {case (k, a) => (k -> a.length)} toArray) sortBy (_._1.length))
      .unzip match {
        case (w, c) =>
          0 until w.length foreach { i =>
            (i + 1) until w.length foreach { j => if (w(j) startsWith w(i)) c(i) += c(j) }
          }
          (w zip c) toMap
      }

  def wordStatForIndex(phrase: String) = normalize(phrase)
    .foldLeft(Map[String, Int]())((stat, w) =>
      (0 until w.length)
        .map(w.dropRight(_))
        .foldLeft(stat)((stat, w) => stat + (w -> stat.get(w).map(_ + 1).getOrElse(1))))

  def extractWords(phrase: String) = wordStatForIndex(phrase)
    .flatMap(t => List(t._1) ++ (2 to t._2).map(s => s"$s*${t._1}"))
}

case class Address(code: Int, address: String, zipCode: String, typ: Int,
  coordX: BigDecimal, coordY: BigDecimal, history: List[String])
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
    if (addressFileName == null || dbConfig.isEmpty)
      logger.error("Address file not set nor database connection parameters specified")
    else {
      if (hasIndex(addressFileName)) loadIndex else {
        val (am, ah) = dbConfig
          .map(db => loadAddressesFromDb(db.url, db.user, db.password, db.driver)) //load from db
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
        (result map address).toArray
      }
    else {
      val words = normalize(str)
      val codes = searchCodes(words)(1024, types)
      var (perfectRankCount, i) = (0, 0)
      val size = Math.min(codes.length, limit)
      val result = new AB[Long](size)
      while (perfectRankCount < size && i < codes.length) {
        val code = codes(i)
        val r = rank(words, code)
        if (r == 0) perfectRankCount += 1
        val key = r.toLong << 53 | i.toLong << 32 | code
        insertIntoHeap(result, key)
        i += 1
      }
      (
        if (size < result.size / 2) heap_topx(result, size)
        else (if (size < result.size) result.sorted.take(size) else result.sorted).toArray
      )
      .map(_ & 0x00000000FFFFFFFFL)
      .map(_.toInt)
      .map(address)
    }
  }

  def searchNearest(coordX: BigDecimal, coordY: BigDecimal)(limit: Int = 1) =
    new Search(Math.min(limit, 20))
      .searchNearest(coordX, coordY)
      .map(result => address(result._1))
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
      .map(address(_))
  }

  def addressOption(code: Int) = addressMap.get(code) map address
  private def address(addrObj: AddrObj): Address = addrObj.foldLeft((
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
      addressHistory.getOrElse(addrObj.code, Nil))
  }
  def address(code: Int): Address = addressOption(code).get

  /**Integer of which last 10 bits are significant.
   * Of them 5 high order bits denote sequential word match count, 5 low bits denote exact word match count.
   * 0 is highest ranking meaning all words sequentially have exact match */
  def rank(words: Array[String], code: Int) = {
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
    na
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

  //for debugging purposes
  def t(block: => Any) = {
    val t1 = System.currentTimeMillis
    block
    System.currentTimeMillis - t1
  }

}

trait AddressIndexerConfig {
  case class DbConfig(driver: String, url: String, user: String, password: String)
  def addressFileName: String
  def blackList: Set[String]
  def houseCoordFile: String
  def dbConfig: Option[DbConfig] = None
}
