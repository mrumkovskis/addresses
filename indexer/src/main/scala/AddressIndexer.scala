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

trait AddressIndexer { this: AddressFinder =>

  type Refs = AB[Ref]

  case class Ref(code: Int, exact: Boolean)
  case class FuzzyResult(refs: Refs, editDistance: Int)

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
    private[this] var multiWordStartIdx = 0
    def updateChildren(w: String, ref: Ref): Unit = {
      if (isEmpty) children = AB()
      val i = binarySearch[MutableIndexNode, String](children, w, _.word, compPrefixes)
      if (i < 0) {
        children.insert(-(i + 1), new MutableIndexNode(w, AB(ref), null))
        if (!w.contains("*")) multiWordStartIdx += 1
      } else {
        children(i).update(w, ref)
      }
    }

    /** Searches index down the tree */
    def apply(str: String): Refs = {
      if(str.contains("*")) {
        val idx = binarySearchFromUntil[MutableIndexNode, String](
          children, searchUntilIdx, children.length, str, _.word, _ compareTo _)
        if (idx < 0) AB()
        else children(idx).refs
      } else search(str)
    }

    /** Searches index down the tree in fuzzy mode */
    def apply(str: String, maxEditDistance: Int): AB[FuzzyResult] = {
      if(str.contains("*")) {
        val idx = binarySearchFromUntil[MutableIndexNode, String](
          children, searchUntilIdx, children.length, str, _.word, _ compareTo _)
        if (idx < 0) AB()
        else AB(FuzzyResult(children(idx).refs, 0))
      } else fuzzySearch(str, 0, maxEditDistance)
    }

    private[MutableIndex] def search(str: String): Refs = {
      if (children == null) return AB()
      val c = str.head
      val idx = binarySearchFromUntil[MutableIndexNode, Char](
        children, 0, searchUntilIdx, c, _.word.head, _ - _)
      if (idx < 0) AB()
      else if (str.length == 1) children(idx).refs
      else children(idx).search(str.drop(1))
    }

    private[AddressIndexer] def fuzzySearch(str: String,
                                            currentEditDistance: Int,
                                            maxEditDistance: Int,
                                            p: String = ""): AB[FuzzyResult] = {
      def tryTransformedSearch(excludeChar: Char) = {
        def replaceOrPrefix(s: String) = {
          var fuzzyResult = AB[FuzzyResult]()
          val l = searchUntilIdx
          var i = 0
          while (i < l && fuzzyResult.isEmpty) {
            if (children(i).word.head != excludeChar) {
              fuzzyResult ++=
                children(i)
                  .fuzzySearch(s, currentEditDistance + 1, maxEditDistance, p + children(i).word)
            }
            i += 1
          }
          fuzzyResult
        }
        //try to prefix c with on of children word values
        replaceOrPrefix(str) ++
          //try to omit c
          fuzzySearch(str drop 1, currentEditDistance + 1, maxEditDistance, p) ++
          //try to replace c with one of children word values
          replaceOrPrefix(str drop 1)
      }

      def mergeTransformedResult(res: AB[FuzzyResult]): AB[FuzzyResult] = {
        res.groupBy(_.editDistance).map[(Refs, Int)] { case (e, refs) => //parametrize map method so that iterable is returned
          merge[Ref](refs.map(_.refs), _.code - _.code) -> e
        }
        .toArray
        .sortBy(_._2)
        .unzip match {
          case (arr, errs) =>
            pruneRight[Ref](AB(scala.collection.immutable.ArraySeq.unsafeWrapArray(arr): _*),
              _.code - _.code
            )
            .zip(errs)
            .withFilter(_._1.nonEmpty)
            .map { case (refs, err) => FuzzyResult(refs, err) }
        }
      }

      val c = str.head
      val idx = binarySearchFromUntil[MutableIndexNode, Char](
        children, 0, searchUntilIdx, c, _.word.head, _ - _)
      if (idx < 0) {
        if (currentEditDistance >= maxEditDistance) AB()
        else mergeTransformedResult(tryTransformedSearch('\u0000')) //no char excluded
      } else {
        val r = children(idx).fuzzySearch(str.drop(1), currentEditDistance, maxEditDistance, p + c)
        if (currentEditDistance >= maxEditDistance) r
        else mergeTransformedResult(r ++ tryTransformedSearch(c)) //exclude found char from further fuzzy search
      }
    }

    private[AddressIndexer] def searchUntilIdx: Int = multiWordStartIdx

    /* Strings are considered equal if they have common prefix but do not have multiplier '*' in them,
    * otherwise standard comparator is used. Words with multiplier are placed at the end. */
    private def compPrefixes(s1: String, s2: String) = {
      val (s1Mult, s2Mult) = (s1.contains("*"), s2.contains("*"))
      val multiplierComp = s1Mult compareTo s2Mult
      if (multiplierComp == 0)
        if (s1Mult || s1(0) != s2(0)) s1 compareTo s2 else 0
      else
        multiplierComp
    }

    def isEmpty = children == null || children.isEmpty
    def nonEmpty = !isEmpty

    /** Restores node from path */
    def load(path: Vector[Int], word: String, refs: Refs): Unit = {
      val idx = path.head
      if (children == null) children = AB()
      if (idx > children.size)
        sys.error(s"Invalid index: $idx, children size: ${children.size}, cannot add node")
      else if (idx == children.size) {
        val n = new MutableIndexNode(null, null, null)
        n.load(path.drop(1), word, refs)
        children += n
        if (!word.contains("*")) multiWordStartIdx += 1 //increase multiword start position
      } else {
        children(idx).load(path.drop(1), word, refs)
      }
    }

    /** Calls writer function while traversing index */
    def write(writer: (Vector[Int], String, Refs) => Unit): Unit = {
      children.zipWithIndex.foreach { case (node, i) => node.writeNode(writer, Vector(i))}
    }

    /** Debuging info */
    def statistics: IndexStats = {
      if (isEmpty)
        IndexStats(0, 0)
      else
        children.foldLeft(IndexStats(children.size, 0)){ (st, n) => st + n.statistics }
    }
    private[AddressIndexer] def validateNodeWord(path: String): AB[(String, String, Int)] = {
      if (isEmpty) AB() else children.flatMap(_.validateNodeWord(path))
    }
    private[AddressIndexer] def validateIndex(path: String): AB[(String, AB[Int])] = {
      if (isEmpty) AB() else children.flatMap(_.validateIndex(path))
    }
    /** Node word must be of one character length if it does not contains multiplier '*'.
     * Returns tuple - (path, word, first address code) */
    def invalidWords: AB[(String, String, Int)] = validateNodeWord("")
    /** Address codes in node must be unique and in ascending order.
     * Returns (invalid path|word, address codes) */
    def invalidIndices: AB[(String, AB[Int])] = validateIndex("")
  }

  final class MutableIndexNode(var word: String, var refs: Refs,
                               _children: AB[MutableIndexNode]) extends MutableIndex(_children) {

    def update(w: String, ref: Ref): Unit = {
      val (commonPart, nodeRest, wordRest) = split(word, w)
      if (nodeRest.isEmpty && wordRest.isEmpty) { //update node codes
        refs.lastOption.filterNot(_ == ref).foreach(_ => refs += ref) //do not add code twice
      } else {
        if (nodeRest.nonEmpty) { //make common part as nodes word, move remaining part deeper
          word = commonPart
          val nc = refs
          refs = AB(ref)
          val nch = children
          children = AB(new MutableIndexNode(nodeRest, nc, nch)) //move children and codes to new child node
        }
        if (wordRest.nonEmpty) { //update children with remaining part of new word
          updateChildren(wordRest, ref)
        }
      }
    }

    override private[AddressIndexer] def fuzzySearch(str: String,
                                                     currentEditDistance: Int,
                                                     maxEditDistance: Int, p: String = ""): AB[FuzzyResult] = {
      def maybe_filter_exact(dist: Int): AB[FuzzyResult] =
        (if (dist >= maxEditDistance) refs.filter(_.exact) else refs) match {
          case r if r.nonEmpty => AB(FuzzyResult(r, dist))
          case _ => AB()
        }

      if (str.isEmpty) {
        if (currentEditDistance > maxEditDistance) AB()
        else maybe_filter_exact(currentEditDistance)
      } else if (children == null) {
        val err = currentEditDistance + str.length
        if (err <= maxEditDistance) {
          maybe_filter_exact(err)
        } else {
          AB()
        }
      } else {
        super.fuzzySearch(str, currentEditDistance, maxEditDistance, p)
      }
    }

    override private[AddressIndexer] def searchUntilIdx: Int = children.length

    /** returns (common part from two args, rest from first arg, rest from second arg) */
    def split(s1: String, s2: String): (String, String, String) = {
      val equalCharCount = s1 zip s2 count (t => t._1 == t._2)
      (s1.substring(0, equalCharCount), s1.substring(equalCharCount), s2.substring(equalCharCount))
    }

    override def load(path: Vector[Int], word: String, refs: Refs): Unit = {
      if (path.isEmpty) {
        this.word = word
        this.refs = refs
      } else {
        super.load(path, word, refs)
      }
    }

    private[AddressIndexer] def writeNode(writer: (Vector[Int], String, Refs) => Unit,
                                          path: Vector[Int]): Unit = {
      writer(path, word, refs)
      if (children != null)
        children.zipWithIndex.foreach { case (node, i) => node.writeNode(writer, path.appended(i))}
    }

    /** Debuging info */
    override def statistics: IndexStats = {
      IndexStats(0, refs.size) + super.statistics
    }

    override private[AddressIndexer] def validateNodeWord(path: String): AB[(String, String, Int)] = {
      (if (word.length > 1 && !word.contains("*")) AB((path, word, _idxCode(refs.head.code))) else AB()) ++
        super.validateNodeWord(path + word)
    }
    override private[AddressIndexer] def validateIndex(path: String): AB[(String, AB[Int])] = {
      val wrongCodes = AB[Int]()
      refs.reduce { (prevCode, curCode) =>
        if (prevCode.code >= curCode.code) wrongCodes += curCode.code
        curCode
      }
      (if (wrongCodes.nonEmpty) AB(s"$path|$word" -> wrongCodes.map(_idxCode)) else AB()) ++
        super.validateIndex(path + word)
    }
  }

  protected var _idxCode: scala.collection.mutable.HashMap[Int, Int] = null
  protected var _index: MutableIndex = null
  //filtering without search string, only by object type code support for (pilsēta, novads, pagasts, ciems)
  protected var _sortedPilsNovPagCiem: Vector[Int] = null

  def searchCodes(words: Array[String])(limit: Int, types: Set[Int] = null): (Array[Int], Int) = {
    def searchParams(words: Array[String]) = wordStatForSearch(words)
      .map { case (w, c) => if (c == 1) w else s"$c*$w" }.toArray
    def idx_vals(word: String) = _index(word)
    def idx_vals_fuzzy(word: String) =
      _index(word,
        if (word.exists(_.isDigit)) 0 //no fuzzy search for words with digits in them
        else WordLengthEditDistances.getOrElse(word.length, DefaultEditDistance)
      ).headOption.getOrElse(FuzzyResult(AB(), 0))
    def has_type(addr_idx: Int) = types(addressMap(_idxCode(addr_idx)).typ)
    def intersect(idx: Array[Refs], limit: Int): Array[Int] = {
      val result = AB[Int]()
      val pos = Array.fill(idx.length)(0)
      def check_register = {
        val v = idx(0)(pos(0)).code
        val l = pos.length
        var i = 1
        while (i < l && v == idx(i)(pos(i)).code) i += 1
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
        val a: Refs = idx(a_pos)
        val b: Refs = idx(b_pos)
        val al = a.length
        val bl = b.length
        var ai = pos(a_pos)
        var bi = pos(b_pos)
        while (ai < al && bi < bl && a(ai).code != b(bi).code)
          if (a(ai).code < b(bi).code) ai += 1 else bi += 1
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

//    def intersectFuzzy(res: Array[FuzzyResult]): FuzzyResult = {
//      val
//    }

    val params = searchParams(words)
    (params map idx_vals sortBy(_.size) match {
      case a if a.isEmpty => (Array[Int](), 0)
      case result => (intersect(result, limit), 0)
    }) match {
      case r @ (a, _) if a.nonEmpty => r
      case _ => //try fuzzy search
        params map idx_vals_fuzzy sortBy { case FuzzyResult(a, e) => (e, a.length) } match {
          case a if a.isEmpty => (Array[Int](), 0)
          case result => (intersect(result.map(_.refs), limit), result.map(_.editDistance).sum)
        }
    }
  }

  def index(addressMap: Map[Int, AddrObj], history: Map[Int, List[String]]) = {

    logger.info("Starting address indexing...")
    logger.info(s"Sorting ${addressMap.size} addresses...")

    val index = new MutableIndex(null)

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
          wordsLists foreach(_ foreach { w =>
            index.updateChildren(w,
              Ref(idx, normalize(name).contains(w)))
          })
          idx_code += (idx -> code)
          idx += 1
          if (idx % 5000 == 0) logger.info(s"Addresses processed: $idx")
      }

    logger.info(s"Address objects processed: $idx; index statistics: ${index.statistics}")

    this._idxCode = idx_code
    this._index = index
  }

  /** Node word must be of one character length if it does not contains multiplier '*'.
   * Returns tuple - (path, word, first address code) */
  def invalidWords: AB[(String, String, Int)] = _index.invalidWords

  /** Address codes in node must be unique and in ascending order.
   * Returns (invalid path|word, address codes) */
  def invalidIndices: AB[(String, AB[Int])] = _index.invalidIndices

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
    binarySearchFromUntil(arr, 0, arr.length, key, keyFunc, comparator)
  }

  def binarySearchFromUntil[T, K](arr: AB[T], fromIdx: Int, toIdx: Int,
                                  key: K, keyFunc: T => K, comparator: (K, K) => Int): Int = {
    var from = fromIdx
    var to = toIdx - 1
    while (from <= to) {
      val i = from + to >>> 1
      val r = comparator(keyFunc(arr(i)), key)
      if (r < 0) from = i + 1 else if (r > 0) to = i - 1 else return i
    }
    -(from + 1)
  }

  /** Merge ordered collections removing duplicates */
  def merge[T >: Null](arrs: AB[AB[T]], comparator: (T, T) => Int): AB[T] = {
    def merge(r1: AB[T], r2: AB[T]) = {
      var i1 = 0
      var i2 = 0
      val l1 = r1.length
      val l2 = r2.length
      val res = new AB[T](Math.max(l1, l2))
      var prevCode: T = null
      while (i1 < l1 && i2 < l2) {
        val c1 = r1(i1)
        val c2 = r2(i2)
        if (comparator(c1, c2) < 0) {
          if (prevCode == null || comparator(prevCode, c1) < 0) {
            res += r1(i1)
            prevCode = c1
          }
          i1 += 1
        } else if (comparator(c1, c2) > 0) {
          if (prevCode == null || comparator(prevCode, c2) < 0) {
            res += r2(i2)
            prevCode = c2
          }
          i2 += 1
        } else {
          if (prevCode == null || comparator(prevCode, c1) < 0) {
            res += r1(i1)
            prevCode = c1
          }
          i1 += 1
          i2 += 1
        }
      }
      def addDistinct(a: AB[T], start: Int, l: Int) = {
        var i = start
        while(i < l) {
          val c = a(i)
          if (prevCode == null || comparator(prevCode, c) < 0) {
            res += a(i)
            prevCode = c
          }
          i += 1
        }
      }
      if (i1 < l1) addDistinct(r1, i1, l1)
      else if (i2 < l2) addDistinct(r2, i2, l2)
      res
    }
    if (arrs.isEmpty) AB()
    else arrs.reduce(merge)
  }

  /** Removes collections values that are in left collection(s) placed to the left, collections are ordered */
  def pruneRight[T](arrs: AB[AB[T]], comparator: (T, T) => Int): AB[AB[T]] = {
    def prune(l: AB[T], r: AB[T]): AB[T] = {
      if (l.isEmpty && r.isEmpty) return r
      var li = 0
      var ri = 0
      val ll = l.length
      while(li < ll && ri < r.length) {
        if (comparator(l(li), r(ri)) < 0) li += 1
        else if (comparator(l(li), r(ri)) > 0) ri += 1
        else {
          r.remove(ri)
          li += 1
        }
      }
      r
    }
    if (arrs.length < 2) return arrs
    var i = 0
    val l = arrs.length
    while (i < l) {
      var j = i + 1
      while (j < l) {
        arrs(j) = prune(arrs(i), arrs(j))
        j += 1
      }
      i += 1
    }
    arrs
  }
}

