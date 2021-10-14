package lv.addresses.index

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.util
import scala.collection.AbstractIterator
import scala.collection.mutable.{ArrayBuffer => AB}
import scala.collection.mutable.{Map => MM}
import scala.language.postfixOps

object Index {

  protected val logger = Logger(LoggerFactory.getLogger("lv.addresses.indexer"))

  sealed case class Refs(exact: AB[Int] = AB(), approx: AB[Int] = AB()) {
    def add(ref: Int, exactMatch: Boolean): Refs = {
      def coll = if (exactMatch) exact else approx

      if (coll.isEmpty) coll += ref
      else
        coll
          .lastOption.filterNot(_ == ref)
          .foreach(_ => coll += ref) //do not add code twice
      this
    }
  }

  sealed case class Result(word: String, refs: AB[Int], editDistance: Int)

  sealed case class FuzzyResult(word: String,
                                refs: AB[Int],
                                editDistance: Int,

                                /** Split count of the original string.
                                 * Corresponds to space count in word field
                                 */
                                splitDepth: Int = 0)

  sealed case class PartialFuzzyResult(word: String,
                                       refs: AB[Int],
                                       editDistance: Int,
                                       rest: String)

  sealed case class IndexStats(nodeStats: NodeStats, repWordStats: AB[NodeStats]) {
    def render: String = s"${nodeStats.render}. Repeated words stats: ${
      repWordStats.zipWithIndex.map { case (s, i) => s"${i + 2}: ${s.render}" }.mkString(", ")
    }"
  }

  sealed case class NodeStats(nodeCount: Long, refCount: Long) {
    def +(s: NodeStats): NodeStats = NodeStats(nodeCount + s.nodeCount, refCount + s.refCount)

    def render: String = s"Node count - $nodeCount, code ref count - $refCount"
  }

  sealed class MutableIndexBase(var children: AB[MutableIndexNode]) {

    def search(str: String): Refs = {
      if (children == null) return Refs()
      val c = str.head
      val idx = binarySearch[MutableIndexNode, Char](children, c, _.word.head, _ - _)
      if (idx < 0) Refs()
      else if (str.length == 1) children(idx).refs
      else children(idx).search(str.drop(1))
    }

    def fuzzySearch(str: String,
                    currentEditDistance: Int,
                    maxEditDistance: Int,
                    consumed: String,
                    partial: MM[String, PartialFuzzyResult],
                    calcMaxEditDist: String => Int): AB[FuzzyResult] = {
      def tryTransformedSearch(excludeChar: Char): AB[FuzzyResult] = {
        def replaceOrPrefix(s: String) = {
          var fuzzyResult = AB[FuzzyResult]()
          val l = children.size
          var i = 0
          while (i < l) {
            val charToTry = children(i).word.head
            if (charToTry != excludeChar) {
              fuzzyResult ++=
                children(i)
                  .fuzzySearch(s, currentEditDistance + 1, maxEditDistance,
                    consumed + charToTry, partial, calcMaxEditDist)
            }
            i += 1
          }
          fuzzyResult
        }

        if (currentEditDistance < maxEditDistance) {
          //try to prefix c with on of children word values
          replaceOrPrefix(str) ++
            //try to omit c
            fuzzySearch(str drop 1, currentEditDistance + 1, maxEditDistance, consumed, partial, calcMaxEditDist) ++
            //try to replace c with one of children word values
            replaceOrPrefix(str drop 1)
        } else AB()
      }

      if (str.isEmpty) return AB()
      val c = str.head
      val idx = binarySearch[MutableIndexNode, Char](children, c, _.word.head, _ - _)
      if (idx < 0) {
        tryTransformedSearch('\u0000') //no char excluded
      } else {
        val r =
          children(idx)
            .fuzzySearch(str.drop(1), currentEditDistance, maxEditDistance, consumed + c, partial, calcMaxEditDist)
        r ++ tryTransformedSearch(c) //exclude found char from further fuzzy search
      }
    }

    def isEmpty = children == null || children.isEmpty

    def nonEmpty = !isEmpty

    def updateChildren(w: String, ref: Int, exact: Boolean): Unit = {
      if (isEmpty) children = AB()
      /* Strings are considered equal if they have common prefix */
      val i = binarySearch[MutableIndexNode, String](children, w, _.word, _.head - _.head)
      if (i < 0) {
        children.insert(-(i + 1), new MutableIndexNode(w, Refs().add(ref, exact), null))
      } else {
        children(i).update(w, ref, exact)
      }
    }

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
      } else {
        children(idx).load(path.drop(1), word, refs)
      }
    }

    /** Calls writer function while traversing index */
    def write(writer: (Vector[Int], String, Refs) => Unit, path: Vector[Int]): Unit = {
      if (children == null) return
      children.zipWithIndex.foreach {
        case (node, i) =>
          val np = path.appended(i)
          node.writeNode(writer, np)
          node.write(writer, np)
      }
    }

    /** Debuging info */
    def nodeStatistics: NodeStats = {
      if (isEmpty)
        NodeStats(0, 0)
      else
        children.foldLeft(NodeStats(children.size, 0)) { (st, n) => st + n.nodeStatistics }
    }

    private[Index] def validateNodeWord(path: String): AB[(String, String, Int)] = {
      if (isEmpty) AB() else children.flatMap(_.validateNodeWord(path))
    }

    private[Index] def validateIndex(path: String): AB[(String, AB[Int])] = {
      if (isEmpty) AB() else children.flatMap(_.validateIndex(path))
    }

    /** Node word must be of one character length if it does not contains multiplier '*'.
     * Returns tuple - (path, word, first address code) */
    def invalidWords: AB[(String, String, Int)] = validateNodeWord("")

    /** Address codes in node must be unique and in ascending order.
     * Returns (invalid path|word, address codes) */
    def invalidIndices: AB[(String, AB[Int])] = validateIndex("")
  }

  final class MutableIndex(_children: AB[MutableIndexNode],
                           var repeatedWordChildren: AB[MutableIndexBase])
    extends MutableIndexBase(_children) {

    /** Searches index down the tree */
    def apply(str: String): Refs = {
      val rep_w_idx = str.indexOf('*')
      if (rep_w_idx != -1) { //repeated word found
        //repeated words start with 2
        val idx = str.substring(0, rep_w_idx).toInt - 2
        if (idx < repeatedWordChildren.size) {
          val ch = repeatedWordChildren(idx)
          if (ch != null) ch.search(str.substring(rep_w_idx + 1)) else Refs()
        } else Refs()
      } else search(str)
    }

    /** Searches index down the tree in fuzzy mode */
    def apply(str: String, maxEditDistance: Int, calcMaxEditDist: String => Int): AB[FuzzyResult] = {
      def fuzzySearchInternal(node: MutableIndexBase, str: String): AB[FuzzyResult] = {
        def reduceResults(res: AB[FuzzyResult]): AB[FuzzyResult] = {
          val r =
            res
              .groupBy(_.word)
              .map[FuzzyResult] { case (_, searchResults) =>
                searchResults.minBy(_.editDistance)
              }
          AB.from(r)
        }

        def completePartial(pr: PartialFuzzyResult, depth: Int): AB[FuzzyResult] = {
          if (pr.refs.isEmpty) return AB()

          val npartialRes = MM[String, PartialFuzzyResult]()
          val nr =
            reduceResults(node.fuzzySearch(pr.rest, 0,
              Math.min(maxEditDistance, calcMaxEditDist(pr.rest)), "", npartialRes, calcMaxEditDist)
            )

          val presMap = MM[String, FuzzyResult]()
          npartialRes.foreach { case (_, npr) =>
            val is = intersect(AB(pr.refs, npr.refs), 1024, null)
            if (is.nonEmpty) {
              val completedRes = completePartial(PartialFuzzyResult(pr.word + " " + npr.word, is,
                pr.editDistance + npr.editDistance, npr.rest), depth + 1)
              completedRes.foreach { fr =>
                //select results with minimal edit distance
                presMap.get(fr.word).map { case FuzzyResult(_, _, ed, _) =>
                  if (fr.editDistance < ed) presMap(fr.word) = fr
                }.getOrElse(presMap.addOne((fr.word, fr)))
              }
            }
          }
          val pres = AB.from(presMap.values)

          if (nr.nonEmpty)
            nr.flatMap { fr =>
              val is = intersect(AB(pr.refs, fr.refs), 1024, null)
              if (is.nonEmpty)
                AB(FuzzyResult(pr.word + " " + fr.word, is, pr.editDistance + fr.editDistance, depth))
              else AB()
            } ++ pres else pres
        }

        val partialRes = MM[String, PartialFuzzyResult]()
        val r = node.fuzzySearch(str,
          0, maxEditDistance, "", partialRes, calcMaxEditDist)
        if (r.isEmpty && partialRes.nonEmpty) {
          val res = AB[FuzzyResult]()
          partialRes.foreach { case (_, pr) =>
            res ++= completePartial(pr, 1)
          }
          res
            //sort by edit distance asc first and then split depth desc and then reference count desc
            .sortBy { fr => (fr.editDistance << 25) - (fr.splitDepth << 24) - fr.refs.length }
        } else {
          reduceResults(r)
            //sort by edit distance asc first and then reference count desc
            .sortBy(fr => (fr.editDistance << 24) - fr.refs.length)
        }
      }

      val rep_w_idx = str.indexOf('*')
      if (rep_w_idx != -1) { //repeated word found
        //repeated words start with 2
        val idx = str.substring(0, rep_w_idx).toInt - 2
        if (idx < repeatedWordChildren.size) {
          val ch = repeatedWordChildren(idx)
          if (ch != null) {
            val wc = s"${idx + 2}*"
            fuzzySearchInternal(ch, str.substring(rep_w_idx + 1))
              .map(fr => fr.copy(word = wc + fr.word))
          } else AB()
        } else AB()
      } else {
        fuzzySearchInternal(this, str)
      }
    }

    override def updateChildren(w: String, ref: Int, exact: Boolean): Unit = {
      if (isEmpty) {
        children = AB()
        repeatedWordChildren = AB()
      }
      val rep_w_idx = w.indexOf('*')
      if (rep_w_idx != -1) {
        // repeated words start with 2
        val idx = w.substring(0, rep_w_idx).toInt - 2
        val repWord = w.substring(rep_w_idx + 1)
        if (repeatedWordChildren.size <= idx) repeatedWordChildren.padToInPlace(idx + 1, null)
        if (repeatedWordChildren(idx) == null) {
          val n = new MutableIndexBase(null)
          n.updateChildren(repWord, ref, exact)
          repeatedWordChildren(idx) = n
        } else repeatedWordChildren(idx).updateChildren(repWord, ref, exact)
      } else {
        super.updateChildren(w, ref, exact)
      }
    }

    override def load(path: Vector[Int], word: String, refs: Refs): Unit = {
      val idx = path.head
      val tail = path.tail
      if (repeatedWordChildren == null) repeatedWordChildren = AB()
      if (idx == -1) super.load(tail, word, refs)
      else if (idx > repeatedWordChildren.size)
        sys.error(s"Invalid index for repeated words: $idx, children size: ${
          repeatedWordChildren.size
        }, cannot add node")
      else if (idx == repeatedWordChildren.size) {
        val n = new MutableIndexBase(null)
        n.load(tail, word, refs)
        repeatedWordChildren += n
      } else {
        repeatedWordChildren(idx).load(tail, word, refs)
      }
    }

    /** Calls writer function while traversing index */
    def write(writer: (Vector[Int], String, Refs) => Unit): Unit = {
      write(writer, Vector(-1))
      repeatedWordChildren.zipWithIndex.foreach { case (node, i) => node.write(writer, Vector(i)) }
    }

    def statistics: IndexStats = {
      val st = nodeStatistics
      val repSt = repeatedWordChildren.map(_.nodeStatistics)
      IndexStats(st, repSt)
    }
  }

  final class MutableIndexNode(var word: String, var refs: Refs,
                               _children: AB[MutableIndexNode]) extends MutableIndexBase(_children) {

    override def fuzzySearch(str: String,
                             currentEditDistance: Int,
                             maxEditDistance: Int,
                             consumed: String,
                             partial: MM[String, PartialFuzzyResult],
                             calcMaxEditDist: String => Int): AB[FuzzyResult] = {
      if (str.isEmpty) {
        //add exact refs to fuzzy result
        (if (currentEditDistance > 0 && refs.exact.isEmpty) AB() else
          AB(FuzzyResult(consumed, if (refs.exact.nonEmpty) refs.exact else refs.approx, currentEditDistance))) ++
          //add children word values if current edit distance is less than max edit distance
          (if (currentEditDistance < maxEditDistance && children != null) {
            children.foldLeft(AB[FuzzyResult]()) { case (r, c) =>
              r ++= c.fuzzySearch(str, currentEditDistance + 1,
                maxEditDistance, consumed + c.word, partial, calcMaxEditDist)
            }
          } else AB())
      } else {
        if (refs.exact.nonEmpty) {
          if (currentEditDistance <= calcMaxEditDist(consumed)) {
            val key = consumed + " " + str

            def partialEntry =
              (key, PartialFuzzyResult(consumed, refs.exact, currentEditDistance + 1 /*space added*/ , str))

            partial.get(key).map { pr =>
              if (currentEditDistance < pr.editDistance) partial += partialEntry
            }.getOrElse(partial += partialEntry)
          }
          if (children == null) {
            val err = currentEditDistance + str.length
            if (err <= maxEditDistance) AB(FuzzyResult(consumed, refs.exact, err))
            else AB()
          } else super.fuzzySearch(str, currentEditDistance, maxEditDistance, consumed, partial, calcMaxEditDist)
        }
        else if (children == null) AB()
        else super.fuzzySearch(str, currentEditDistance, maxEditDistance, consumed, partial, calcMaxEditDist)
      }
    }

    def update(w: String, ref: Int, exact: Boolean): Unit = {
      val (commonPart, nodeRest, wordRest) = split(word, w)
      if (nodeRest.isEmpty && wordRest.isEmpty) { //update node codes
        refs.add(ref, exact)
      } else {
        if (nodeRest.nonEmpty) { //make common part as nodes word, move remaining part deeper
          word = commonPart
          val onlyRep = commonPart.endsWith("*")
          val chRefs = if (onlyRep) refs.add(ref, exact) else refs
          refs = if (onlyRep) null else Refs().add(ref, exact)
          val nch = children
          children = AB(new MutableIndexNode(nodeRest, chRefs, nch)) //move children and refs to new child node
        }
        if (wordRest.nonEmpty) { //update children with remaining part of new word
          updateChildren(wordRest, ref, exact)
        }
      }
    }

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

    def writeNode(writer: (Vector[Int], String, Refs) => Unit, path: Vector[Int]): Unit = {
      writer(path, word, refs)
    }

    /** Debuging info */
    override def nodeStatistics: NodeStats = Option(refs).map { r =>
      NodeStats(0, r.exact.size + r.approx.size)
    }.getOrElse(NodeStats(0, 0)) + super.nodeStatistics

    override def validateNodeWord(path: String): AB[(String, String, Int)] = {
      (if (word.length > 1 && !word.contains("*"))
        AB((path, word, refs.exact.headOption.getOrElse(refs.approx.head)))
      else AB()) ++
        super.validateNodeWord(path + word)
    }

    override def validateIndex(path: String): AB[(String, AB[Int])] = {
      val wrongCodes = AB[Int]()

      def findDuplicates(arr: AB[Int]): Int = {
        if (arr.isEmpty) return -1
        arr.reduce { (prevCode, curCode) =>
          if (prevCode >= curCode) wrongCodes += curCode
          curCode
        }
      }

      (if (refs != null) {
        findDuplicates(refs.exact)
        findDuplicates(refs.approx)
        if (wrongCodes.nonEmpty) AB(s"$path|$word" -> wrongCodes) else AB()
      } else AB()) ++ super.validateIndex(path + word)
    }
  }

  def searchCodes(words: AB[String],
                  index: MutableIndex,
                  calcMaxEditDist: String => Int)(limit: Int,
                                                  filter: Int => Boolean = null): AB[Result] = {
    def searchParams(words: AB[String]) = {
      val ws =
        wordStatForSearch(words)
          .groupBy(_._1)
          .map[(String, Int)] { case (_, s) => s.sortBy(-_._2).head }
          .map { case (w, c) => if (c == 1) w else s"$c*$w" }
      AB.from(ws)
    }

    def search_idx(word: String) = index(word)

    def search_idx_fuzzy(word: String) = {
      index(word, calcMaxEditDist(word), calcMaxEditDist)
    }

    def exactSearch(p: AB[String], editDistance: Int, exactMatchWords: Set[String]): AB[Result] = {
      val result = p.map(w => search_idx(w) -> exactMatchWords(w))
        .map {
          case (r, true) => AB(r.exact)
          case (r, false) => AB(r.exact, r.approx).filter(_.nonEmpty)
        }
      var refCount = 0
      val intersection = AB[Int]()
      val combInit = (AB.fill[AB[Int]](result.size)(null), 0) //(refs, idx)
      foldCombinations[AB[Int], (AB[AB[Int]], Int), AB[Int]](result,
        combInit,
        (cr, d) => {
          cr._1(cr._2) = d
          (cr._1, cr._2 + 1)
        },
        intersection,
        (r, cr) => {
          val intr = intersect(cr._1, limit, filter)
          r ++= intr
          refCount += intr.length
          (r, refCount < limit)
        }
      )
      intersection match {
        case a if a.isEmpty => AB()
        case a => AB(Result(p.mkString(" "), if (a.size > limit) a.take(limit) else a, editDistance))
      }
    }

    def exactSearchWithMerge(params: AB[String]): AB[Result] = {
      if (params.isEmpty) AB()
      else {
        var count = 0
        var result = AB[Result]()
        binCombinations(params.size - 1, spaces => {
          var i = 1
          var j = 0
          var editDistance = 0
          val ab = AB[String](params(0))
          spaces foreach { s =>
            if (s == 0) { //separate word
              ab += params(i)
              j += 1
            } else { //merge words
              ab(j) = ab(j) + params(i)
              editDistance += 1
            }
            i += 1
          }
          result = exactSearch(searchParams(ab), editDistance, Set())
          count += 1
          result.isEmpty && count < 32
        })
        result
      }
    }

    exactSearchWithMerge(words) match {
      case a if a.nonEmpty => a
      case _ => // fuzzy search
        val params = searchParams(words)
        //reset ref count
        var refCount = 0
        val fuzzyRes = params map search_idx_fuzzy
        var productiveIntersectionCount = 0
        var intersectionCount = 0

        val MaxProductiveIntersectionCount = 32
        val MaxIntersectionCount = 1024
        foldCombinations[FuzzyResult, AB[FuzzyResult], AB[Result]](
          fuzzyRes,
          AB(),
          (frs, fr) => frs += fr,
          AB[Result](),
          (r, frs) => {
            val res = exactSearch(
              searchParams(frs
                .flatMap(fr => if (fr.splitDepth == 0) AB(fr.word) else AB.from(fr.word.split(' ')))),
              frs.foldLeft(0)((ed, fr) => ed + fr.editDistance),
              frs.collect { case fr if fr.editDistance > 0 => fr.word }.toSet
            )
            if (res.nonEmpty) {
              r ++= res
              refCount += res.foldLeft(0)(_ + _.refs.length)
              productiveIntersectionCount += 1
            }
            intersectionCount += 1
            if (intersectionCount >= MaxIntersectionCount && productiveIntersectionCount == 0)
              logger.debug(s"A LOT OF FUZZY RESULTS: ${fuzzyRes.map(_.size).mkString("(", ",", ")")}\n ${
                fuzzyRes.map(_.map(fr => fr.word -> fr.editDistance)
                  .mkString("(", ",", ")")).mkString("[", ",", "]")
              }")

            (r, refCount < limit &&
              intersectionCount < MaxIntersectionCount &&
              productiveIntersectionCount < MaxProductiveIntersectionCount
            )
          }
        )
    }
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
    .foldLeft(AB[scala.collection.mutable.StringBuilder]() -> true) {
      case ((s, b), c) =>
        if (isWhitespaceOrSeparator(c)) (s, true) else {
          if (b) s.append(new scala.collection.mutable.StringBuilder)
          s.last.append(accents.getOrElse(c, c))
          (s, false)
        }
    }._1
    .map(_.toString)

  def wordStatForSearch(words: AB[String]): AB[(String, Int)] = {
    val stats = AB.fill(words.size)(1)
    0 until words.size foreach { i =>
      (i + 1) until words.size foreach { j =>
        if (words(j).length > words(i).length) {
          if (words(j) startsWith words(i)) stats(i) += 1
        } else if (words(i) startsWith words(j)) stats(j) += 1
      }
    }
    words zip stats
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
      while (x < lx) {
        vals(x) = x
        x += 1
      }
      x = 1
      var y = 1
      var dist = 0
      while (y < ly) {
        var xm1 = y
        while (x < lx) {
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

  def intersect(idx: AB[AB[Int]], limit: Int, filter: Int => Boolean): AB[Int] = {
    val result = AB[Int]()
    val pos = Array.fill(idx.length)(0)

    def check_register = {
      val v = idx(0)(pos(0))
      val l = pos.length
      var i = 1
      while (i < l && v == idx(i)(pos(i))) i += 1
      if (i == l) {
        if (filter == null || filter(v)) result append v
        i = 0
        while (i < l) {
          pos(i) += 1
          i += 1
        }
      }
    }

    def find_equal(a_pos: Int, b_pos: Int) = {
      def search(arr: AB[Int], from: Int, until: Int, code: Int) = {
        val i = binarySearchFromUntil[Int, Int](arr, from, until, code, identity _, _ - _)
        if (i < 0) -(i + 1) else i
      }

      val a: AB[Int] = idx(a_pos)
      val b: AB[Int] = idx(b_pos)
      val al = a.length
      val bl = b.length
      var ai = pos(a_pos)
      var bi = pos(b_pos)
      while (ai < al && bi < bl && a(ai) != b(bi))
        if (a(ai) < b(bi)) ai = search(a, ai + 1, al, b(bi))
        else bi = search(b, bi + 1, bl, a(ai))
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
      while (i < l) {
        find_equal(i, i + 1)
        i += 1
      }
    }
    result
  }

  def hasIntersection(idx: AB[AB[Int]]): Boolean = {
    val pos = Array.fill(idx.length)(0)
    var hasCommon = false

    def checkEquals = {
      val v = idx(0)(pos(0))
      val l = pos.length
      var i = 1
      while (i < l && v == idx(i)(pos(i))) i += 1
      hasCommon = i == l
      hasCommon
    }

    def find_equal(a_pos: Int, b_pos: Int) = {
      def search(arr: AB[Int], from: Int, until: Int, code: Int) = {
        val i = binarySearchFromUntil[Int, Int](arr, from, until, code, identity _, _ - _)
        if (i < 0) -(i + 1) else i
      }

      val a: AB[Int] = idx(a_pos)
      val b: AB[Int] = idx(b_pos)
      val al = a.length
      val bl = b.length
      var ai = pos(a_pos)
      var bi = pos(b_pos)
      while (ai < al && bi < bl && a(ai) != b(bi))
        if (a(ai) < b(bi)) ai = search(a, ai + 1, al, b(bi))
        else bi = search(b, bi + 1, bl, a(ai))
      pos(a_pos) = ai
      pos(b_pos) = bi
    }

    def continue = {
      var i = 0
      val l = pos.length
      while (i < l && pos(i) < idx(i).length) i += 1
      i == l && !checkEquals
    }

    while (continue) {
      var i = 0
      val l = pos.length - 1
      while (i < l) {
        find_equal(i, i + 1)
        i += 1
      }
    }
    hasCommon
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
        while (i < l) {
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
      while (li < ll && ri < r.length) {
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

  def foldCombinations[A, B, C](data: AB[AB[A]],
                                combInit: => B,
                                combFun: (B, A) => B,
                                init: C,
                                folder: (C, B) => (C, Boolean)): C = {
    if (data.exists(_.isEmpty)) return init
    data.sortInPlaceBy(_.size)
    val count = data.size
    var res = init
    val limits = Array.tabulate(count)(data(_).size - 1)
    val max_sum = limits.sum
    var s = 0
    var continue = true
    while(s <= max_sum && continue) {
      val it = new PermutationsItrOverSumEls(count, s, limits)
      while (continue && it.hasNext) {
        val idxs = it.next()
        var combRes = combInit
        var i = 0
        while (i < count) {
          combRes = combFun(combRes, data(i)(idxs(i)))
          i += 1
        }
        val (fr, cont) = folder(res, combRes)
        res = fr
        continue = cont
      }
      s += 1
    }
    res
  }

  class PermutationsItrOverSumEls(el_count: Int, sum: Int,
                                 limits: Array[Int] /** NOTE: limits must be sorted! */)
      extends AbstractIterator[Array[Int]] {

    private val elms = new Array[Int](el_count) //y
    private val cur_l = new Array[Int](el_count) //l
    private val cur_s = new Array[Int](el_count) //s
    private val result = new Array[Int](el_count) //res
    private val permutator = new PermutationsItr(el_count, limits)
    private var _hasNext = el_count > 0 && init

    private def init: Boolean = {
      var i = el_count - 1
      var s = sum
      cur_l(i) = Math.min(limits(i), s)
      while (i >= 0) {
        cur_s(i) = s
        if (i == 0) {
          elms(0) = s
          cur_l(0) = limits(0)
        } else {
          elms(i) = Math.min(s, cur_l(i))
          val in = i - 1
          s = s - elms(i)
          cur_l(in) = Math.min(limits(in), elms(i))
        }
        i -= 1
      }
      s <= cur_l(0)
    }
    private def cont_adjust(idx: Int): Boolean = {
      var i = idx
      elms(i) -= 1
      i -= 1
      while (i >= 0) {
        val ip = i + 1
        cur_s(i) = cur_s(ip) - elms(ip)
        cur_l(i) = Math.min(limits(i), elms(ip))
        elms(i) = Math.min(cur_s(i), cur_l(i))
        i -= 1
      }
      cur_s(0) > cur_l(0)
    }
    private def calc_next: Boolean = {
      var i = 1
      while (i < el_count && cont_adjust(i)) i += 1
      i < el_count
    }

    def hasNext = _hasNext || permutator.hasNext
    @throws[NoSuchElementException]
    def next(): Array[Int] = {
      if (!hasNext)
        Iterator.empty.next()

      if (permutator.hasNext) permutator.next()
      else {
        System.arraycopy(elms, 0, result, 0, el_count)
        permutator.init(result)
        permutator.next()
        _hasNext = calc_next
        result
      }
    }

    //recursive implementation of element combinations generation for sum
//    private def se(c: Int, s: Int, l: List[Int]): (List[List[Int]], Int) = {
//      if (c == 0) (Nil, 0)
//      else if (c == 1) {
//        if (s <= l.head) (List(List(s)), s)
//        else (Nil, 0)
//      } else {
//        def r(y: Int, res: List[List[Int]]): (List[List[Int]], Int) = {
//          se(c - 1, s - y, Math.min(l.tail.head, y) :: l.tail.tail) match {
//            case (x, z) if x.nonEmpty && y >= z => r(y - 1, res ::: x.map(y :: _))
//            case _ => (res, y)
//          }
//        }
//        r(Math.min(s, l.head), Nil)
//      }
//    }

    //ported from scala standard library
    class PermutationsItr(size: Int, limits: Array[Int]) extends AbstractIterator[Array[Int]] {
      private[this] var result: Array[Int] = _
      private[this] val elms: Array[Int] = new Array[Int](size)
      private[this] var _hasNext = false

      def hasNext = _hasNext
      @throws[NoSuchElementException]
      def next(): Array[Int] = {
        if (!hasNext)
          Iterator.empty.next()

        System.arraycopy(elms, 0, result, 0, size)
        var i = elms.length - 2
        while(i >= 0 && elms(i) >= elms(i + 1)) i -= 1

        if (i < 0)
          _hasNext = false
        else {
          var j = elms.length - 1
          while(elms(j) <= elms(i)) j -= 1
          if (elms(j) > limits(i)) { // check limit
            j = i - 1
            while (j >= 0 && (elms(j + 1) > limits(j) || elms(j + 1) <= elms(j))) j -= 1
            if (j < 0) _hasNext = false
            else swap(j, j + 1)
          } else swap(i, j) // limit ok

          val len = (elms.length - i) / 2
          var k = 1
          while (k <= len) {
            swap(i + k, elms.length - k)
            k += 1
          }
        }
        result
      }
      private def swap(i: Int, j: Int): Unit = {
        val tmpE = elms(i)
        elms(i) = elms(j)
        elms(j) = tmpE
      }

      def init(res: Array[Int]) = {
        // NOTE: res must be sorted!
        result = res
        _hasNext = true
        //val m = scala.collection.mutable.HashMap[Int, Int]()
        //idxs = result map (m.getOrElseUpdate(_, m.size))
        //util.Arrays.sort(idxs)
        System.arraycopy(res, 0, elms, 0, size)
      }
    }
  }

  def binCombinations(n: Int, f: Array[Int] => Boolean): Unit = {
    val a = new Array[Int](n)
    var b = true

    def go(d: Int): Unit = {
      if (d == 0) b = f(a.clone) else {
        var i = 0
        while (i <= 1 && b) {
          a(n - d) = i
          go(d - 1)
          i += 1
        }
      }
    }

    go(n)
  }
}
