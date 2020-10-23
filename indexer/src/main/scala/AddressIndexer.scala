package lv.addresses.indexer

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

  case class IndexStats(nodeCount: Long, refCount: Long) {
    def +(s: IndexStats): IndexStats = IndexStats(nodeCount + s.nodeCount, refCount + s.refCount)
    def render: String = s"Node count - $nodeCount, code ref count - $refCount"
  }

  sealed class MutableIndex(var children: AB[MutableIndexNode]) {
    def updateChildren(w: String, code: Int): Unit = {
      if (isEmpty) children = AB()
      if (w.contains("*")) { //repeating words - do not split
        val i = bin_search(children, w, _.word, comp)
        if (i < 0)
          children.insert(-(i + 1), new MutableIndexNode(w, AB(code),null))
        else
          children(i).codes += code
      } else {
        val i = bin_search(children, w, _.word, compPrefixes)
        if (i < 0) {
          children.insert(-(i + 1), new MutableIndexNode(w, AB(code), null))
        } else {
          children(i).update(w, code)
        }
      }
    }

    /** Searches index down the tree */
    def apply(str: String): AB[Int] = {
      if(str.contains("*")) {
        val idx = bin_search(children, str, _.word, comp)
        if (idx < 0) AB()
        else children(idx).codes
      } else search(str)
    }

    private[MutableIndex] def search(str: String): AB[Int] = {
      val c = str.head
      val idx = binarySearch[MutableIndexNode, Char](children, c, _.word.head, _ - _)
      if (idx < 0) AB()
      else if (str.length == 1) children(idx).codes
      else children(idx).search(str.drop(1))
    }

    private def bin_search = binarySearch[MutableIndexNode, String] _

    private def comp(s1: String, s2: String) = s1.compareTo(s2)
    /* Strings are considered equal if they have common prefix */
    private def compPrefixes(s1: String, s2: String) = if (s1(0) == s2(0)) 0 else s1.compareTo(s2)

    def isEmpty = children == null || children.size == 0
    def nonEmpty = !isEmpty

    /** Restores node from path */
    def load(path: Vector[Int], word: String, codes: AB[Int]): Unit = {
      val idx = path.head
      if (children == null) children = AB()
      if (idx > children.size)
        sys.error(s"Invalid index: $idx, children size: ${children.size}, cannot add node")
      else if (idx == children.size) {
        val n = new MutableIndexNode(null, null, null)
        n.load(path.drop(1), word, codes)
        children += n
      } else {
        children(idx).load(path.drop(1), word, codes)
      }
    }

    /** Calls writer function while traversing index */
    def write(writer: (Vector[Int], String, AB[Int]) => Unit): Unit = {
      children.zipWithIndex.foreach { case (node, i) => node.writeNode(writer, Vector(i))}
    }

    /** Debuging info */
    def statistics: IndexStats = {
      if (isEmpty)
        IndexStats(0, 0)
      else
        children.foldLeft(IndexStats(children.size, 0)){ (st, n) => st + n.statistics }
    }
  }

  final class MutableIndexNode(var word: String, var codes: AB[Int],
                               _children: AB[MutableIndexNode]) extends MutableIndex(_children) {

    def update(w: String, code: Int): Unit = {
      val (commonPart, nodeRest, wordRest) = split(word, w)
      if (nodeRest.isEmpty && wordRest.isEmpty) { //update node codes
        codes.lastOption.filterNot(_ == code).foreach(_ => codes += code) //do not add code twice
      } else {
        if (nodeRest.nonEmpty) { //make common part as nodes word, move remaining part deeper
          word = commonPart
          val nc = codes
          codes = AB(code)
          val nch = children
          children = AB(new MutableIndexNode(nodeRest, nc, nch)) //move children and codes to new child node
        }
        if (wordRest.nonEmpty) { //update children with remaining part of new word
          updateChildren(wordRest, code)
        }
      }
    }

    /** returns (common part from two args, rest from first arg, rest from second arg) */
    def split(s1: String, s2: String): (String, String, String) = {
      val equalCharCount = s1 zip s2 count (t => t._1 == t._2)
      (s1.substring(0, equalCharCount), s1.substring(equalCharCount), s2.substring(equalCharCount))
    }

    /** Debuging info */
    override def statistics: IndexStats = {
      IndexStats(0, codes.size) + super.statistics
    }

    override def load(path: Vector[Int], word: String, codes: AB[Int]): Unit = {
      if (path.isEmpty) {
        this.word = word
        this.codes = codes
      } else {
        super.load(path, word, codes)
      }
    }

    private[AddressIndexer] def writeNode(writer: (Vector[Int], String, AB[Int]) => Unit,
                                          path: Vector[Int]): Unit = {
      writer(path, word, codes)
      if (children != null)
        children.zipWithIndex.foreach { case (node, i) => node.writeNode(writer, path.appended(i))}
    }
  }

  protected var _idxCode: scala.collection.mutable.HashMap[Int, Int] = null
  protected var _index: MutableIndex = new MutableIndex(null)
  //filtering without search string, only by object type code support for (pilsēta, novads, pagasts, ciems)
  protected var _sortedPilsNovPagCiem: Vector[Int] = null

  def searchCodes(words: Array[String])(limit: Int, types: Set[Int] = null): Array[Int] = {
    def searchParams(words: Array[String]) = wordStatForSearch(words)
      .map { case (w, c) => if (c == 1) w else s"$c*$w" }.toArray
    def idx_vals(word: String) = _index(word)
    def has_type(addr_idx: Int) = types(addressMap(_idxCode(addr_idx)).typ)
    def intersect(idx: Array[AB[Int]], limit: Int): Array[Int] = {
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
        val a: AB[Int] = idx(a_pos)
        val b: AB[Int] = idx(b_pos)
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
    sortedAddresses
      .foreach {
        case (code, _, name) =>
          val wordsLists = extractWords(name) ::
            history.getOrElse(code, Nil).map(extractWords)
          wordsLists foreach(_ foreach(_index.updateChildren(_, idx)))
          idx_code += (idx -> code)
          idx += 1
          if (idx % 5000 == 0) logger.info(s"Addresses processed: $idx")
      }

    logger.info(s"Address objects processed: $idx; index statistics: ${_index.statistics}")

    this._idxCode = idx_code
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
    ((words groupBy identity map {case (k, a) => k -> a.length} toArray) sortBy (_._1.length))
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

  def editDistance(s1: String, s2: String): Int = {
    val lx = s1.length + 1
    val ly = s2.length + 1
    if (lx > 1 && ly > 1) {
      val vals = new Array[Int](lx)
      var x = 0
      while(x < lx) {
        vals(x) = x
        x += 1
      }
      x = 1
      var y = 1
      var dist = 0
      while(y < ly) {
        var xm1 = y
        while(x < lx) {
          val dxy = vals(x - 1) + (if (s1.charAt(x - 1) == s2.charAt(y - 1)) 0 else 1)
          val d = Math.min(xm1 + 1, Math.min(vals(x) + 1, dxy))
          vals(x - 1) = xm1
          xm1 = d
          x += 1
        }
        x = 1
        y += 1
        dist = xm1
      }
      dist
    } else Math.max(lx - 1, ly - 1)
  }

  def binarySearch[T, K](arr: AB[T], key: K, keyFunc: T => K, comparator: (K, K) => Int): Int = {
    var from = 0
    var to = arr.length - 1
    while (from <= to) {
      val i = from + to >>> 1
      val r = comparator(keyFunc(arr(i)), key)
      if (r < 0) from = i + 1 else if (r > 0) to = i - 1 else return i
    }
    -(from + 1)
  }
}
