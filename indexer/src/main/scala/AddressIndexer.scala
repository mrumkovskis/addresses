package lv.addresses.indexer

import java.util.Properties

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

trait AddressIndexer { this: AddressFinder =>

  case class Refs(exact: AB[Int] = AB(), approx: AB[Int] = AB()) {
    def add(ref: Int, exactMatch: Boolean): Refs = {
      def coll = if (exactMatch) exact else approx
      if (coll.size == 0) coll += ref
      else
        coll
          .lastOption.filterNot(_ == ref)
          .foreach(_ => coll += ref) //do not add code twice
      this
    }
  }
  case class FuzzyResult(word: String, refs: Refs, editDistance: Int)

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
    def updateChildren(w: String, ref: Int, exact: Boolean): Unit = {
      if (isEmpty) children = AB()
      val i = binarySearch[MutableIndexNode, String](children, w, _.word, compPrefixes)
      if (i < 0) {
        children.insert(-(i + 1), new MutableIndexNode(w, Refs().add(ref, exact), null))
        if (!w.contains("*")) multiWordStartIdx += 1
      } else {
        children(i).update(w, ref, exact)
      }
    }

    /** Searches index down the tree */
    def apply(str: String): Refs = {
      if(str.contains("*")) {
        val idx = binarySearchFromUntil[MutableIndexNode, String](
          children, searchUntilIdx, children.length, str, _.word, _ compareTo _)
        if (idx < 0) Refs()
        else children(idx).refs
      } else search(str)
    }

    /** Searches index down the tree in fuzzy mode */
    def apply(str: String, maxEditDistance: Int): AB[FuzzyResult] = {
      if(str.contains("*")) {
        val idx = binarySearchFromUntil[MutableIndexNode, String](
          children, searchUntilIdx, children.length, str, _.word, _ compareTo _)
        if (idx < 0) AB()
        else AB(FuzzyResult(children(idx).word, children(idx).refs, 0))
      } else {
        val r =
          fuzzySearch(str, 0, maxEditDistance)
            .groupBy(_.word)
            .map[FuzzyResult] { case (_, searchResults) =>
              searchResults.minBy(_.editDistance)
            }
        AB.from(r).sortBy(_.editDistance)
      }
    }

    private[MutableIndex] def search(str: String): Refs = {
      if (children == null) return Refs()
      val c = str.head
      val idx = binarySearchFromUntil[MutableIndexNode, Char](
        children, 0, searchUntilIdx, c, _.word.head, _ - _)
      if (idx < 0) Refs()
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
          while (i < l) {
            val charToTry = children(i).word.head
            if (charToTry != excludeChar) {
              fuzzyResult ++=
                children(i)
                  .fuzzySearch(s, currentEditDistance + 1, maxEditDistance,
                    p + charToTry)
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

      if (str.isEmpty) return AB()
      val c = str.head
      val idx = binarySearchFromUntil[MutableIndexNode, Char](
        children, 0, searchUntilIdx, c, _.word.head, _ - _)
      if (idx < 0) {
        if (currentEditDistance >= maxEditDistance) AB()
        else tryTransformedSearch('\u0000') //no char excluded
      } else {
        val r =
          children(idx)
            .fuzzySearch(str.drop(1), currentEditDistance, maxEditDistance, p + c)
        if (currentEditDistance >= maxEditDistance) r
        else r ++ tryTransformedSearch(c) //exclude found char from further fuzzy search
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

    def update(w: String, ref: Int, exact: Boolean): Unit = {
      val (commonPart, nodeRest, wordRest) = split(word, w)
      if (nodeRest.isEmpty && wordRest.isEmpty) { //update node codes
        refs.add(ref, exact)
      } else {
        if (nodeRest.nonEmpty) { //make common part as nodes word, move remaining part deeper
          word = commonPart
          val chRefs = refs
          refs = Refs().add(ref, exact)
          val nch = children
          children = AB(new MutableIndexNode(nodeRest, chRefs, nch)) //move children and refs to new child node
        }
        if (wordRest.nonEmpty) { //update children with remaining part of new word
          updateChildren(wordRest, ref, exact)
        }
      }
    }

    override private[AddressIndexer] def fuzzySearch(str: String,
                                                     currentEditDistance: Int,
                                                     maxEditDistance: Int, p: String = ""): AB[FuzzyResult] = {

      if (str.isEmpty) {
        if (currentEditDistance > maxEditDistance) AB()
        else {
          //add exact refs to fuzzy result
          (if (refs.exact.isEmpty) AB() else AB(FuzzyResult(p, refs, currentEditDistance))) ++
          //add children word values if current edit distance is less than max edit distance
          (if (currentEditDistance < maxEditDistance && children != null)  {
            children.foldLeft(AB[FuzzyResult]()) { case (r, c) =>
              r ++= c.fuzzySearch(str, currentEditDistance + 1, maxEditDistance, p + c.word)
            }
          } else AB())
        }
      } else if (children == null) {
        val err = currentEditDistance + str.length
        if (err <= maxEditDistance && refs.exact.nonEmpty) AB(FuzzyResult(p, refs, err))
        else AB()
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
      IndexStats(0, refs.exact.size + refs.approx.size) + super.statistics
    }

    override private[AddressIndexer] def validateNodeWord(path: String): AB[(String, String, Int)] = {
      (if (word.length > 1 && !word.contains("*"))
        AB((path, word, _idxCode(refs.exact.headOption.getOrElse(refs.approx.head))))
      else AB()) ++
        super.validateNodeWord(path + word)
    }
    override private[AddressIndexer] def validateIndex(path: String): AB[(String, AB[Int])] = {
      val wrongCodes = AB[Int]()
      def findDuplicates(arr: AB[Int]): Int = {
        if (arr.isEmpty) return -1
        arr.reduce { (prevCode, curCode) =>
          if (prevCode >= curCode) wrongCodes += curCode
          curCode
        }
      }
      findDuplicates(refs.exact)
      findDuplicates(refs.approx)
      (if (wrongCodes.nonEmpty) AB(s"$path|$word" -> wrongCodes.map(_idxCode)) else AB()) ++
        super.validateIndex(path + word)
    }
  }

  protected var _idxCode: scala.collection.mutable.HashMap[Int, Int] = null
  protected var _index: MutableIndex = null
  def idxCode = _idxCode
  def indexNode = _index
  //filtering without search string, only by object type code support for (pilsēta, novads, pagasts, ciems)
  protected var _sortedPilsNovPagCiem: Vector[Int] = null

  /** Returns array of typles, each tuple consist of mathcing address codes and
   * edit distance from search parameters */
  def searchCodes(words: Array[String])(limit: Int, types: Set[Int] = null): AB[(AB[Int], Int)] = {
    def searchParams(words: Array[String]) = wordStatForSearch(words)
      .map { case (w, c) => if (c == 1) w else s"$c*$w" }.toArray
    def idx_vals(word: String) = _index(word)
    def idx_vals_fuzzy(word: String) = {
      _index(word,
        if (word.exists(_.isDigit)) 0 //no fuzzy search for words with digits in them
        else WordLengthEditDistances.getOrElse(word.length, DefaultEditDistance)
      )
    }

    def has_type(addr_idx: Int) = types(addressMap(_idxCode(addr_idx)).typ)
    def intersect(idx: Array[AB[Int]], limit: Int): AB[Int] = {
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
        while (ai < al && bi < bl && a(ai) != b(bi))
          if (a(ai) < b(bi)) ai += 1 else bi += 1
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
      result.map(i => _idxCode(i))
    }

    val params = searchParams(words)
    (params map idx_vals).map(r => AB(r.exact, r.approx).filter(_.nonEmpty)) match {
      case a if a.isEmpty => AB[(AB[Int], Int)]()
      case result =>
        var refCount = 0
        val intersection = AB[Int]()
        val combInit = (new Array[AB[Int]](result.size), 0) //(refs, idx)
        foldCombinations[AB[Int], (Array[AB[Int]], Int), AB[Int]](result,
          combInit,
          (cr, d) => {
            cr._1(cr._2) = d
            (cr._1, cr._2 + 1)
          },
          intersection,
          (r, cr) => {
            val intr = intersect(cr._1, limit)
            r ++= intr
            refCount += intr.length
            (r, refCount < limit)
          }
        )
        intersection match {
          case intersected if intersected.nonEmpty => //exact result found
            AB((if (intersected.size > limit) intersected.take(limit) else intersected, 0))
          case _ =>
            //reset ref count
            refCount = 0
            val fullRes = params map idx_vals_fuzzy
            val fuzzyRes = fullRes
              .map(_.map(fr => (fr.word, fr.refs.exact, fr.editDistance))) //pick only exact refs for fuzzy result
            val fuzzyIntersection = AB[(AB[Int], Int)]()
            var productiveIntersectionCount = 0
            var intersectionCount = 0
            val fuzzyCombInit = (new Array[AB[Int]](result.size), 0, 0) //(refs, editDistance, idx)
            val currentWords = Array.fill(fullRes.size)("")
            val MaxProductiveIntersectionCount = 32
            val MaxIntersectionCount = 1024
            foldCombinations[(String, AB[Int], Int), (Array[AB[Int]], Int, Int), AB[(AB[Int], Int)]](
              fuzzyRes,
              fuzzyCombInit,
              (cr, d) => {
                currentWords(cr._3) = d._1 //for debugging purposes
                cr._1(cr._3) = d._2
                (cr._1, cr._2 + d._3, cr._3 + 1) //sum all edit distances from fuzzy results
              },
              fuzzyIntersection,
              (r, cr) => {
                val int_ed = (intersect(cr._1, limit), cr._2)
                if (int_ed._1.nonEmpty) {
                  r += int_ed
                  refCount += int_ed._1.length
                  productiveIntersectionCount += 1
                }
                intersectionCount += 1
                //println(s"INTERSECTION: ${currentWords.mkString(",")}, ${int_ed._1.size}")
                if (intersectionCount > MaxIntersectionCount)
                  logger.debug(s"A LOT OF FUZZY RESULTS: ${fullRes.map(_.size).mkString("(", ",", ")")}\n ${
                    fullRes.map(_.map(fr => fr.word -> fr.editDistance)
                      .mkString("(", ",", ")")).mkString(",")}")

                (r, refCount < limit &&
                    intersectionCount < MaxIntersectionCount &&
                    productiveIntersectionCount < MaxProductiveIntersectionCount
                )
              }
            )
            val res =
              fuzzyIntersection
                .groupBy(_._2)
                .map[(AB[Int], Int)] {
                  case (err, refs) => //parametrize map method so that iterable is returned
                    merge(refs.map(_._1)) -> err
                }
                .toArray
                .sortBy(_._2)
                .unzip match {
                  case (arr, errs) =>
                    pruneRight(AB(scala.collection.immutable.ArraySeq.unsafeWrapArray(arr): _*))
                      .zip (errs)
                      .filter(_._1.nonEmpty)
                }
            if (res.size > limit) res.take(limit) else res
        }
    }
  }

  def fuzzySearch(str: String, editDistance: Int): AB[FuzzyResult] = {
    _index(str, editDistance)
  }

  def index(addressMap: Map[Int, AddrObj], history: Map[Int, List[String]]) = {

    logger.info("Starting address indexing...")
    logger.info("Loading address synonyms...")
    val synonyms: Properties = {
      val props = new Properties()
      val in = getClass.getResourceAsStream("/synonyms.properties")
      if (in != null) props.load(in)
      props
    }
    logger.info(s"${synonyms.size} address synonym(s) loaded")

    logger.info(s"Sorting ${addressMap.size} addresses...")

    val index = new MutableIndex(null)

    //(addressCode, ordering weight, full space separated unnaccented address)
    var idx = 0
    val sortedAddresses = {
      def typeOrder(code: Int, typ: Int) = typ match {
        case PIL if !addressMap.contains(addressMap(code).superCode) => 0 // top level cities
        case _ => typeOrderMap(typ)
      }
      val addresses = new Array[(Int, Int, String)](addressMap.size)
      addressMap.foreach { case (code, addr) =>
        addresses(idx) = (code, typeOrder(code, addr.typ) * 100 + addr.depth,
          addr.foldRight(AB[String]()){(b, o) => b += unaccent(o.name)}.mkString(" "))
        idx += 1
      }
      addresses.sortWith { case ((_, ord1, a1), (_, ord2, a2)) =>
        ord1 < ord2 || (ord1 == ord2 && (a1.length < a2.length || (a1.length == a2.length && a1 < a2)))
      }
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
            index.updateChildren(w, idx, normalize(name).contains(w))
            //update synonyms
            Option(synonyms.getProperty(w))
              .foreach(syn => extractWords(syn).foreach(index.updateChildren(_, idx, true)))
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
  def merge(arrs: AB[AB[Int]]): AB[Int] = {
    def merge(r1: AB[Int], r2: AB[Int]) = {
      var i1 = 0
      var i2 = 0
      val l1 = r1.length
      val l2 = r2.length
      val res = new AB[Int](Math.max(l1, l2))
      var prevCode = -1
      while (i1 < l1 && i2 < l2) {
        val c1 = r1(i1)
        val c2 = r2(i2)
        if (c1 < c2) {
          if (prevCode < c1) {
            res += r1(i1)
            prevCode = c1
          }
          i1 += 1
        } else if (c1 > c2) {
          if (prevCode < c2) {
            res += r2(i2)
            prevCode = c2
          }
          i2 += 1
        } else {
          if (prevCode < c1) {
            res += r1(i1)
            prevCode = c1
          }
          i1 += 1
          i2 += 1
        }
      }
      def addDistinct(a: AB[Int], start: Int, l: Int) = {
        var i = start
        while(i < l) {
          val c = a(i)
          if (prevCode < c) {
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

  /** Removes duplicates from collections going forward i.e. from collection at index 1 are removed elements
   * contained by collection at index 0, and so on.
   * Collections are ordered */
  def pruneRight(arrs: AB[AB[Int]]): AB[AB[Int]] = {
    def prune(l: AB[Int], r: AB[Int]): AB[Int] = {
      if (l.isEmpty && r.isEmpty) return r
      var li = 0
      var ri = 0
      val ll = l.length
      while(li < ll && ri < r.length) {
        if (l(li) < r(ri)) li += 1
        else if (l(li) > r(ri)) ri += 1
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

  def foldCombinations[A, B, C](data: Array[AB[A]],
                                combInit: B,
                                combFun: (B, A) => B,
                                init: C,
                                folder: (C, B) => (C, Boolean)): C = {
    if (data.exists(_.isEmpty)) return init
    val count = data.length
    val ls = data.map(_.length - 1)
    val pos = Array.fill(count)(0)
    var res = init
    var continue = true
    def neq = {
      var i = 0
      while (i < count && pos(i) == 0) i += 1
      i != count && continue
    }
    do {
      var combRes = combInit
      var i = 0
      while (i < count) {
        combRes = combFun(combRes, data(i)(pos(i)))
        i += 1
      }
      val (fr, cont) = folder(res, combRes)
      res = fr
      continue = cont
      i = 0
      var shift = true
      while (i < count && shift) {
        if (pos(i) == ls(i)) {
          pos(i) = 0
        } else {
          pos(i) += 1
          shift = false
        }
        i += 1
      }
    } while(neq)
    init
  }
}
