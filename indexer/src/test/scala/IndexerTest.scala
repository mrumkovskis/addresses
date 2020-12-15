package lv.addresses.indexer

import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import spray.json._

import scala.io.Source

import lv.addresses.index.Index._

object IndexerTest {
  class TestAddressFinder(val addressFileName: String, val blackList: Set[String],
                          val houseCoordFile: String, val dbConfig: Option[DbConfig]) extends AddressFinder

  val finder = new TestAddressFinder(null, Set.empty, null, None)

  object IndexJsonProtocol extends DefaultJsonProtocol {
    implicit def abFormat[T: JsonFormat] = new RootJsonFormat[ArrayBuffer[T]] {
      override def write(obj: ArrayBuffer[T]): JsValue = obj.toVector.toJson
      override def read(json: JsValue): ArrayBuffer[T] = ArrayBuffer.from(json.convertTo[Vector[T]])
    }
    implicit object NodeFormat extends RootJsonFormat[MutableIndexNode] {
      implicit val refFormat = jsonFormat2(Refs)
      override def write(obj: MutableIndexNode): JsValue = {
        import obj._

        JsObject(Map(
          "word" -> Option(word).map(JsString(_)).getOrElse(JsNull),
          "refs" -> Option(refs).map(_.toJson).getOrElse(JsNull),
          "children" -> NodeBaseFormat.write(obj)
        ))
      }

      override def read(json: JsValue): MutableIndexNode = (json: @unchecked) match {
        case JsObject(fields) =>
          new MutableIndexNode(
            fields("word").convertTo[String],
            fields("refs").convertTo[Refs],
            fields("children").convertTo[ArrayBuffer[MutableIndexNode]]
          )
      }
    }
    implicit object NodeBaseFormat extends RootJsonFormat[MutableIndexBase] {
      override def write(obj: MutableIndexBase): JsValue = {
        Option(obj.children).map(_.toJson).getOrElse(JsNull)
      }

      override def read(json: JsValue): MutableIndexBase = (json: @unchecked) match {
        case nodes => new MutableIndexBase(nodes.convertTo[ArrayBuffer[MutableIndexNode]])
      }
    }
    implicit object IndexFormat extends RootJsonFormat[MutableIndex] {
      override def write(obj: MutableIndex): JsValue = {
        import obj._
        JsObject(Map(
          "index" -> Option(children).map(_.toJson).getOrElse(JsNull),
          "repeated_words_index" ->
            Option(repeatedWordChildren).map(_.toJson).getOrElse(JsNull)
        ))
      }

      override def read(json: JsValue): MutableIndex = (json: @unchecked) match {
        case JsObject(fields) =>
          def nodes[T: JsonFormat](name: String) = fields(name).convertTo[ArrayBuffer[T]]
          new MutableIndex(nodes[MutableIndexNode]("index"),
            nodes[MutableIndexBase]("repeated_words_index"))
      }
    }
  }
}

class IndexerTest extends FunSuite {

  import IndexerTest._

  test("word stat for indexer") {
    assertResult(Map("vid" -> 1, "n" -> 1, "vidussko" -> 1, "pa" -> 1, "vall" -> 3, "vi" -> 1,
      "vecumniek" -> 1, "vecu" -> 1, "vecumn" -> 1, "viduss" -> 1, "no" -> 1, "vec" -> 1,
      "vecumnie" -> 1, "nov" -> 1, "vidusskola" -> 1,
      "vidu" -> 1, "va" -> 3, "vecumni" -> 1, "vidus" -> 1,
      "v" -> 5, "pag" -> 1, "valle" -> 3,
      "vecum" -> 1, "ve" -> 1, "vidusskol" -> 1, "vidussk" -> 1, "p" -> 1, "val" -> 3,
      "vecumnieku" -> 1, "valles" -> 2)) {
        wordStatForIndex("Valles vidusskola, Valle, Valles pag., Vecumnieku nov.")
    }
  }

  test("word stat for search") {
    assertResult(Array(("Valles", 1), ("vidusskola", 1), ("Valle", 3),
      ("Valles" ,2), ("pag", 1), ("Vecumnieku", 1), ("nov", 1)))(
      wordStatForSearch(ArrayBuffer("Valles", "vidusskola", "Valle", "Valles", "pag", "Vecumnieku", "nov")))
  }

  test("index") {
    val node = List(
      "aknas",
      "akls",
      "ak ak",
      "aknīste",
      "ak aknīste",
      "aka aka",
      "aka akācijas",
      "21 215"
    )
    .zipWithIndex
    .foldLeft(new MutableIndex(null, null)) { (node, addrWithIdx) =>
      val (addr, idx) = addrWithIdx
      val words = extractWords(addr)
      //check that duplicate word do not result in duplicate address code reference
      (if (words.exists(_ == "ak")) words ++ List("ak") else words)
        .foreach { w =>
          val exactStr = if (w.contains("*")) w.substring(w.indexOf('*') + 1) else w
          node.updateChildren(w, idx, normalize(addr).contains(exactStr))
        }
      node
    }

    val expectedResult = Source
      .fromInputStream(this.getClass.getResourceAsStream("/index_node.json"), "UTF-8")
      .mkString
      .parseJson

    import IndexJsonProtocol._
    //println(node.toJson.prettyPrint)
    assertResult(expectedResult)(node.toJson)

    assertResult(IndexStats(NodeStats(20,39),ArrayBuffer(NodeStats(5,12))))(node.statistics)
    assertResult(ArrayBuffer())(node.invalidIndices)
    assertResult(ArrayBuffer())(node.invalidWords)
  }

  test("index search") {
    val node = new MutableIndex(null, null)
    val idx_val = List(
      "aknas",
      "akls",
      "ak ak",
      "aknīste",
      "ak aknīste",
      "aka aka",
      "aka akācijas",
      "21 215",
      "ventspils",
      "vencīši",
      "venskalni",
      "ventilācijas",
      "kazdanga",
      "ķirši",
      "ķiršu",
      "ķirša",
      "rīga",
      "brīvības",
      "brīvības rīga",
      "brīvības iela rīga",
      "brīvības valka",
      "brīvības iela valka",
      "brīvības iela 2 valka",
      "valles vidusskola, valle"
    )
    .zipWithIndex
    .map { case (addr, idx) =>
      val words = extractWords(addr)
      words.foreach { w =>
        val exactStr = if (w.contains("*")) w.substring(w.indexOf('*') + 1) else w
        node.updateChildren(w, idx, normalize(addr).contains(exactStr))
      }
      (idx, addr)
    }.toMap

    def word(str: String) = normalize(str).head
    def search_exact(str: String) = res(node(str))
    def search_fuzzy(str: String, ed: Int) =
      res_fuzzy(node(str, ed, finder.maxEditDistance))
    def res(refs: Refs) = (refs.exact ++ refs.approx).map(idx_val(_)).toList
    def res_fuzzy(res: ArrayBuffer[FuzzyResult]) = {
      res.map { case FuzzyResult(_, r, ed, sd) => (r.map(idx_val(_)).toList, ed, sd) }.toList
    }

    def search(str: String) =
      searchCodes(normalize(str), node, finder.maxEditDistance)(1024)
        .map(r => (r.refs.map(idx_val).toList, r.editDistance)).toList

    //exact search, edit distance 0
    assertResult(List("ak ak", "ak aknīste", "aknas", "akls", "aknīste", "aka aka", "aka akācijas"))(search_exact("ak"))
    assertResult(List("ak ak", "ak aknīste", "aka aka", "aka akācijas"))(search_exact("2*ak"))
    assertResult(List("aknas"))(search_exact("akna"))
    assertResult(Nil)(search_exact("aknass"))
    assertResult(Nil)(search_exact("ziz"))

    //fuzzy search, edit distance 1
    assertResult(List((List("aka aka", "aka akācijas"), 0, 0),
      (List("ak ak", "ak aknīste"), 1, 0)))(search_fuzzy(word("aka"), 1))
    assertResult(List((List("aka akācijas"), 0, 0)))(search_fuzzy(word("akācijas"), 1))
    assertResult(List((List("aka akācijas"), 1, 0)))(search_fuzzy(word("akcijas"), 1))
    assertResult(List((List("aka akācijas"), 1, 0)))(search_fuzzy(word("akucijas"), 1))
    assertResult(List((List("aka akācijas"), 1, 0)))(search_fuzzy(word("akaicijas"), 1))
    assertResult(List((List("aka akācijas"), 1, 0)))(search_fuzzy(word("kakācijas"), 1))
    assertResult(List((List("aka akācijas"), 1, 0)))(search_fuzzy(word("ukācijas"), 1))
    assertResult(List((List("aka akācijas"), 1, 0)))(search_fuzzy(word("akācijs"), 1))
    assertResult(List((List("aka akācijas"), 1, 0)))(search_fuzzy(word("akācijaz"), 1))
    assertResult(List((List("aka akācijas"), 1, 0)))(search_fuzzy(word("akācijass"), 1))
    assertResult(List((List("akls"),1, 0), (List("aknas"),1, 0)))(search_fuzzy(word("aklas"), 1))
    assertResult(List((List("akls"), 1, 0)))(search_fuzzy(word("kakls"), 1))
    assertResult(List((List("ventspils"), 0, 0)))(search_fuzzy(word("ventspils"), 1))
    assertResult(List((List("ventspils"), 1, 0)))(search_fuzzy(word("venspils"), 1))
    assertResult(Nil)(search_fuzzy(word("vencpils"), 1))
    assertResult(Nil)(search_fuzzy(word("kaklas"), 1))

    assertResult(List((List("ventspils"), 1, 0)))(search_fuzzy(word("venspils"), 1))
    assertResult(List((List("ventspils"), 1, 0)))(search_fuzzy(word("ventpils"), 1))
    assertResult(Nil)(search_fuzzy(word("venpils"), 1))
    assertResult(List((List("kazdanga"), 1, 0)))(search_fuzzy(word("bazdanga"), 1))
    assertResult(List((List("ventspils"), 1, 0)))(search_fuzzy(word("bentspils"), 1))
    assertResult(List((List("kazdanga"), 1, 0)))(search_fuzzy(word("vazdanga"), 1))
    assertResult(List((List("ventspils"), 1, 0)))(search_fuzzy(word("kentspils"), 1))
    assertResult(List((List("ķirša"), 0, 0), (List("ķiršu"), 1, 0), (List("ķirši"), 1, 0)))(search_fuzzy(word("kirsa"), 1))
    assertResult(List((List("valles vidusskola, valle"), 1, 0)))(search_fuzzy(word("2*vallez"), 1))

    //fuzzy search, edit distance 2
    assertResult(List((List("akls"), 2, 0)))(search_fuzzy(word("akliss"), 2))
    assertResult(List((List("akls"), 2, 0), (List("aknas"), 2, 0)))(search_fuzzy(word("kaklas"), 2))
    assertResult(List((List("akls"), 2, 0)))(search_fuzzy(word("kikls"), 2))
    assertResult(List((List("akls"), 2, 0)))(search_fuzzy(word("akliss"), 2))
    assertResult(Nil)(search_fuzzy(word("kakliss"), 2))
    assertResult(List((List("ventspils"), 1, 0)))(search_fuzzy(word("ventpils"), 2))
    assertResult(List((List("ventspils"), 1, 0)))(search_fuzzy(word("venspils"), 2))
    assertResult(List((List("ventspils"), 2, 0)))(search_fuzzy(word("venpils"), 2))
    assertResult(List((List("ventspils"), 2, 0)))(search_fuzzy(word("vencpils"), 2))
    assertResult(List((List("kazdanga"), 2, 0)))(search_fuzzy(word("sbazdanga"), 2))
    assertResult(List((List("kazdanga"), 2, 0)))(search_fuzzy(word("kazdangazi"), 2))

    //fuzzy search with search string split
    //spaces are included in edit distance calculation
    assertResult(List((List("brīvības rīga", "brīvības iela rīga"), 1, 1)))(search_fuzzy(word("rigabrivibas"), 0))
    assertResult(List((List("brīvības rīga", "brīvības iela rīga"), 4, 1)))(search_fuzzy(word("riiabrvbas"), 2))
    assertResult(List((List("brīvības rīga", "brīvības iela rīga"), 2, 1)))(search_fuzzy(word("riiabriv"), 2))
    assertResult(List((List("brīvības rīga", "brīvības iela rīga"), 2, 1)))(search_fuzzy(word("riiabrivi"), 2))
    assertResult(List((List("brīvības iela rīga"), 2, 1)))(search_fuzzy(word("riiaie"), 2))
    assertResult(List((List("brīvības iela rīga"), 3, 2)))(search_fuzzy(word("riiabrivibasiela"), 2))
    assertResult(List((List("brīvības iela rīga", "brīvības iela valka", "brīvības iela 2 valka"), 3, 1)))(search_fuzzy(word("brvbasiela"), 2))
    assertResult(List((List("brīvības iela rīga"), 5, 2)))(search_fuzzy(word("riiabrvbasiela"), 2))
    assertResult(List((List("brīvības iela rīga"), 5, 2)))(search_fuzzy(word("riiaielabrvbas"), 2))
    assertResult(List((List("brīvības iela valka", "brīvības iela 2 valka"), 5, 2)))(search_fuzzy(word("vlkabrvbasiela"), 2))
    assertResult(List((List("brīvības iela 2 valka"), 5, 3),(List("brīvības iela valka", "brīvības iela 2 valka"), 5, 2)))(search_fuzzy(word("brvbasiela2valka"), 2))

    //fuzzy search merge words
    assertResult(List((List("aknas"), 1)))(search("ak nas"))
    assertResult(List((List("aknas"), 4)))(search("a k n a s"))
    //repeated word search
    assertResult(List((List("valles vidusskola, valle"), 1)))(search("vallez vallez"))
  }
}
