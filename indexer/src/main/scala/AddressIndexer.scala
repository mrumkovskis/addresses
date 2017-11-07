package lv.addresses.indexer

import scala.collection.JavaConverters._
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
    PIL -> 1, //pilsēta
    NOV -> 2, //novads
    PAG -> 3, //pagasts
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

  protected var _index: scala.collection.mutable.HashMap[String, Array[Long]] = null
  //filtering without search string, only by object type code support for (pilsēta, novads, pagasts, ciems)
  protected var _sortedPilsNovPagCiem: Vector[Int] = null

  def searchCodes(words: Array[String])(limit: Int, types: Set[Int] = null) = {
    def searchParams(words: Array[String]) = wordStatForSearch(words)
      .map(t => if (t._2 == 1) t._1 else t._2 + "*" + t._1).toArray
    def idx_vals(word: String) = _index.getOrElse(word, Array[Long]())
    def address_from_idx_key(k: Long) = addressMap((k & 0x00000000FFFFFFFFL).asInstanceOf[Int])
    def big_units(word: String) =
      idx_vals(word).takeWhile(c => big_unit_types.contains(address_from_idx_key(c).typ))
    def find_equal(a: Array[Long], b: Array[Long], a_pos: Int, b_pos: Int, idxs: Array[Int]) = {
      val al = a.length
      val bl = b.length
      var ai = idxs(a_pos)
      var bi = idxs(b_pos)
      while (ai < al && bi < bl && a(ai) != b(bi)) {
        if (a(ai) < b(bi)) ai += 1 else bi += 1
      }
      idxs(a_pos) = ai
      idxs(b_pos) = bi
    }
    def merge(codes1: AB[Long], codes2: Array[Long], types: Set[Int]): AB[Long] = {
      val nr = AB[Long]()
      var (i, j) = (0, 0)
      while (i < codes1.length && j < codes2.length) {
        var rv = codes1(i)
        var av = codes2(j)
        if (rv == av) {
          i += 1
          j += 1
          if (types == null || types.contains(address_from_idx_key(rv).typ)) nr.append(rv)
        } else if (rv < av) i += 1 else j += 1
      }
      nr
    }

    (words match {
      case Array() =>
        Array[Long]()
      case Array(word) =>
        //for one word search take only big address units which are at the beginning of array
        val result = idx_vals(word)
        if (result.length > limit) result take limit else result
      case words: Array[String] =>
        (searchParams(words) map idx_vals sortBy(_.size)) match {
          case Array() => Array[Long]()
          case Array(codes) => codes
          case codes => codes.tail.foldLeft(AB[Long]() ++ codes.head)(merge(_, _, types)).toArray
        }
    }).map(_ & 0x00000000FFFFFFFFL).map(_.toInt)
  }

  def index(addressMap: Map[Int, AddrObj]) = {

    println("Starting address indexing...")
    val start = System.currentTimeMillis

    println(s"Sorting ${addressMap.size} addresses...")
    val addresses = new Array[(Int, Int, String)](addressMap.size)
    var idx = 0
    addressMap.foreach { case (code, addr) =>
      addresses(idx) = (code, typeOrderMap(addr.typ) * 100 + addr.depth,
          addr.foldRight(new scala.collection.mutable.StringBuilder())((b, o) =>
            b.append(" ").append(unaccent(o.name))).toString)
      idx += 1
    }
    val sortedAddresses = addresses.sortWith((a1, a2) => a1._2 < a2._2 || (a1._2 == a2._2 &&
        (a1._3.length < a2._3.length || (a1._3.length == a2._3.length && a1._3 < a2._3))))

    _sortedPilsNovPagCiem =
      sortedAddresses
        .collect { case (a, _, _) if big_unit_types.contains(addressMap(a).typ) => a }
        .toVector
    println(s"Total size of pilseta, novads, pagasts, ciems - ${_sortedPilsNovPagCiem.size}")

    println("Creating index...")
    idx = 0
    val index = sortedAddresses
      .foldLeft(scala.collection.mutable.HashMap[String, AB[Long]]())(
        (index, addrTuple) => {
          def ref(idx: Long, code: Long) = (idx << 32) | code
          val o = addressMap(addrTuple._1)
          val r = ref(idx, o.code)
          val words = extractWords(addrTuple._3)
          words foreach (w => {
            if (index contains w) index(w).append(r)
            else index(w) = AB(r)
          })
          idx += 1
          if (idx % 5000 == 0) println(s"Addresses processed: $idx; word cache size: ${index.size}")
          index
        })
      .map(t => t._1 -> t._2.toArray)

    val refCount = index.foldLeft(0L)((c, t) => c + t._2.size)
    val end = System.currentTimeMillis
    println(s"Address objects processed: $idx; word cache size: ${index.size}; ref count: $refCount, ${end - start}ms")

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

  def wordStatForSearch(words: Array[String]) = words
    .foldLeft(Map[String, Int]()) { (stat, w) =>
      val (new_stat, this_w_count) = stat.foldLeft(stat -> 0) { case ((ns, tc), (cw, cc)) =>
        if (w startsWith cw) (ns + (cw -> (cc + 1)), tc)
        else if (cw startsWith w) (ns, tc + 1) else (ns, tc)
      }
      if (new_stat.contains(w)) new_stat else new_stat + (w -> (this_w_count + 1))
    }

  def wordStatForIndex(phrase: String) = normalize(phrase)
    .foldLeft(Map[String, Int]())((stat, w) =>
      (0 until w.length)
        .map(w.dropRight(_))
        .foldLeft(stat)((stat, w) => stat + (w -> stat.get(w).map(_ + 1).getOrElse(1))))

  def extractWords(phrase: String) = wordStatForIndex(phrase)
    .flatMap(t => List(t._1) ++ (2 to t._2).map(_ + "*" + t._1))
}

trait AddressLoader { this: AddressFinder =>

  val files: Map[String, (Array[String]) => AddrObj] =
    Map("AW_CIEMS.CSV" -> conv _,
      "AW_DZIV.CSV" -> conv_dziv _,
      "AW_IELA.CSV" -> conv _,
      "AW_NLIETA.CSV" -> conv_nlt _,
      "AW_NOVADS.CSV" -> conv _,
      "AW_PAGASTS.CSV" -> conv _,
      "AW_PILSETA.CSV" -> conv _,
      "AW_RAJONS.CSV" -> conv _).filter(t => !(blackList contains t._1))

  def conv(line: Array[String]) = AddrObj(line(0).toInt, line(1).toInt, line(2), line(3).toInt, null,
      normalize(line(2)).toVector)
  def conv_nlt(line: Array[String]) = AddrObj(line(0).toInt, line(1).toInt, line(7), line(5).toInt, line(9),
      normalize(line(7)).toVector)
  def conv_dziv(line: Array[String]) = AddrObj(line(0).toInt, line(1).toInt, line(7), line(5).toInt, null,
      normalize(line(7)).toVector)

  def loadAddresses(addressZipFile: String = addressFileName, hcf: String = houseCoordFile) = {
    println(s"Loading addreses from file $addressZipFile, house coordinates from file $houseCoordFile...")
    val start = System.currentTimeMillis
    var currentFile: String = null
    var converter: Array[String] => AddrObj = null
    val f = new java.util.zip.ZipFile(addressZipFile)
    val houseCoords = Option(hcf).flatMap(cf => Option(f.getEntry(cf)))
      .map{e =>
        println(s"Loading house coordinates $hcf ...")
        scala.io.Source.fromInputStream(f.getInputStream(e))
      }.toList
      .flatMap(_.getLines.drop(1))
      .map{r =>
        val coords = r.split(";").map(_.drop(1).dropRight(1))
        coords(1).toInt -> (BigDecimal(coords(2)) -> BigDecimal(coords(3)))
      }.toMap
    val addressMap = f.entries.asScala
      .filter(files contains _.getName)
      .map(f => { println(s"loading file: $f"); converter = files(f.getName); currentFile = f.getName; f })
      .map(f.getInputStream(_))
      .map(scala.io.Source.fromInputStream(_, "Cp1257"))
      .flatMap(_.getLines.drop(1))
      .filter(l => {
        val r = l.split(";")
        //use only existing addresses - status: EKS
        (if (Set("AW_NLIETA.CSV", "AW_DZIV.CSV") contains currentFile) r(2) else r(7)) == "#EKS#"
      })
      .map(r => converter(r.split(";").map(_.drop(1).dropRight(1))))
      .map(o => o.code -> houseCoords
        .get(o.code)
        .map(coords => o.copy(coordX = coords._1).copy(coordY = coords._2))
        .getOrElse(o))
      .toMap
    f.close
    println(s"${addressMap.size} addresses loaded in ${System.currentTimeMillis - start}ms")
    addressMap
  }

}

trait AddressIndexLoader { this: AddressIndexer =>
  import java.io._
  def save(addressMap: Map[Int, AddrObj],
    index: scala.collection.mutable.Map[String, Array[Long]],
    sortedPilNovPagCiem: Vector[Int],
    akFileName: String) = {

    println(s"Saving address index for $akFileName...")
    val start = System.currentTimeMillis
    val idxFile = indexFile(akFileName)
    if (idxFile.exists) sys.error(s"Cannot save address index file. File $idxFile already exists")
    val maxRefArray = index.maxBy(_._2.length)
    val maxRefArrayLength = maxRefArray._2.length
    println(s"Max. reference array length for the word '${maxRefArray._1}': ${maxRefArray._2.length}")
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

    val addrFile = addressCacheFile(akFileName)
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
    println(s"Address index saved in ${System.currentTimeMillis - start}ms")
  }
  def load(akFileName: String) = {
    println(s"Loading address index for $akFileName...")
    val start = System.currentTimeMillis
    val idxFile = indexFile(akFileName)
    if (!idxFile.exists) sys.error(s"Index file $idxFile not found")
    val in = new DataInputStream(new BufferedInputStream(new FileInputStream(idxFile)))
    val index = scala.collection.mutable.HashMap[String, Array[Long]]()
    var c = 0
    var spnpc = new Array[Int](in.readInt)
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
    println(s"Total words loaded: $c")
    val addrFile = addressCacheFile(akFileName)
    if (!addrFile.exists) sys.error(s"Address file $addrFile not found")
    var addressMap = Map[Int, AddrObj]()
    var ac = 0
    scala.io.Source.fromInputStream(new BufferedInputStream(new FileInputStream(addrFile)), "UTF-8")
      .getLines
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
    println(s"Address index loaded (words - $c, addresses - $ac) in ${System.currentTimeMillis - start}ms")
    (addressMap, index, spnpc.toVector)
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

case class Address(code: Int, address: String, zipCode: String, typ: Int,
  coordX: BigDecimal, coordY: BigDecimal)
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

  private[this] var _addressMap: Map[Int, AddrObj] = null

  def addressMap = _addressMap

  def init: Unit = {
    if (ready) return
    if (addressFileName == null) println("Address file not set")
    else {
      if (hasIndex(addressFileName)) loadIndex else {
        _addressMap = loadAddresses()
        index(addressMap)
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
     a.typ,
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
    Address(addrObj.code, as.toString, zip, typ, coordX, coordY)
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
        }
        idx += 1
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
    save(addressMap, _index, _sortedPilsNovPagCiem, addressFileName)
  }

  def loadIndex = {
    val r = load(addressFileName)
    _addressMap = r._1
    _index = r._2
    _sortedPilsNovPagCiem = r._3
  }

  def insertIntoHeap(h: AB[Long], el: Long) {
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
  def addressFileName: String
  def blackList: Set[String]
  def houseCoordFile: String
}
