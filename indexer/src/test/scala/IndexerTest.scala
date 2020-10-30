package lv.addresses.indexer

import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import spray.json._

import scala.io.Source

object IndexerTest {
  class TestAddressFinder(val addressFileName: String, val blackList: Set[String],
                          val houseCoordFile: String, val dbConfig: Option[DbConfig]) extends AddressFinder

  val finder = new TestAddressFinder(null, Set.empty, null, None)

  object IndexJsonProtocol extends DefaultJsonProtocol {
    implicit object NodeFormat extends RootJsonFormat[finder.MutableIndexNode] {
      implicit val refFormat = jsonFormat2(finder.Ref)
      override def write(obj: finder.MutableIndexNode): JsValue = {
        import obj._
        JsObject(Map(
          "word" -> Option(word).map(JsString(_)).getOrElse(JsNull),
          "refs" -> Option(refs).map(_.toVector.toJson).getOrElse(JsNull),
          "children" -> Option(children).map(_.map(this.write).toVector).map(JsArray(_)).getOrElse(JsNull)
        ))
      }

      override def read(json: JsValue): finder.MutableIndexNode = (json: @unchecked) match {
        case JsObject(fields) =>
          new finder.MutableIndexNode(
            fields("word").convertTo[String],
            ArrayBuffer.from(fields("refs").convertTo[Vector[finder.Ref]]),
            ArrayBuffer.from((fields("children"): @unchecked) match { case JsArray(ch) => ch.map(read)})
          )
      }
    }
    implicit object IndexFormat extends RootJsonFormat[finder.MutableIndex] {
      override def write(obj: finder.MutableIndex): JsValue = {
        import obj._
        JsArray(Option(children).map(_.map(_.toJson).toVector).getOrElse(Vector()))
      }

      override def read(json: JsValue): finder.MutableIndex = (json: @unchecked) match {
        case JsArray(children) =>
          new finder.MutableIndex(ArrayBuffer.from(children.map(_.convertTo[finder.MutableIndexNode])))
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
        finder.wordStatForIndex("Valles vidusskola, Valle, Valles pag., Vecumnieku nov.")
    }
  }

  test("word stat for search") {
    assertResult(Map("nov" -> 1, "Valles" -> 2, "pag" -> 1, "Vecumnieku" -> 1, "vidusskola" -> 1, "Valle" -> 3))(
      finder.wordStatForSearch(Array("Valles", "vidusskola", "Valle", "Valles", "pag", "Vecumnieku", "nov")))
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
    .foldLeft(new finder.MutableIndex(ArrayBuffer())) { (node, addrWithIdx) =>
      val (addr, idx) = addrWithIdx
      val words = finder.extractWords(addr)
      //check that duplicate word do not result in duplicate address code reference
      (if (words.exists(_ == "ak")) words ++ List("ak") else words)
        .foreach(w => node.updateChildren(w, finder.Ref(idx, finder.normalize(addr).contains(w))))
      node
    }

    val expectedResult = Source
      .fromInputStream(this.getClass.getResourceAsStream("/index_node.json"), "UTF-8")
      .mkString
      .parseJson

    import IndexJsonProtocol._
    //println(node.toJson.prettyPrint)
    assertResult(expectedResult)(node.toJson)

    assertResult(finder.IndexStats(25,51))(node.statistics)
    assertResult(ArrayBuffer())(node.invalidIndices)
    assertResult(ArrayBuffer())(node.invalidWords)
  }

  test("index search") {
    val node = new finder.MutableIndex(ArrayBuffer())
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
      "kazdanga"
    )
    .zipWithIndex
    .map { case (addr, idx) =>
      val words = finder.extractWords(addr)
      words.foreach(w => node.updateChildren(w, finder.Ref(idx, finder.normalize(addr).contains(w))))
      (idx, addr)
    }.toMap

    def word(str: String) = finder.normalize(str).head
    def search(str: String) = res(node(str))
    def search_fuzzy(str: String, ed: Int) = res_fuzzy(node(str, ed))
    def res(refs: ArrayBuffer[finder.Ref]) = refs.map(r => idx_val(r.code)).toList
    def res_fuzzy(res: (ArrayBuffer[finder.Ref], Int)) = {
      (res._1.map(r => idx_val(r.code)).toList, res._2)
    }

    //exact search, edit distance 0
    assertResult(List("aknas", "akls", "ak ak", "aknīste", "ak aknīste", "aka aka", "aka akācijas"))(search("ak"))
    assertResult(List("ak ak", "ak aknīste", "aka aka", "aka akācijas"))(search("2*ak"))
    assertResult(List("aknas"))(search("akna"))
    assertResult(Nil)(search("aknass"))
    assertResult(Nil)(search("ziz"))

    //fuzzy search, edit distance 1
    assertResult(((List("aka aka", "aka akācijas"),0)))(search_fuzzy(word("aka"), 1))
    assertResult((List("aka akācijas"), 0))(search_fuzzy(word("akācijas"), 1))
    assertResult((List("aka akācijas"), 1))(search_fuzzy(word("akcijas"), 1))
    assertResult((List("aka akācijas"), 1))(search_fuzzy(word("akucijas"), 1))
    assertResult((List("aka akācijas"), 1))(search_fuzzy(word("akaicijas"), 1))
    assertResult((List("aka akācijas"), 1))(search_fuzzy(word("kakācijas"), 1))
    assertResult((List("aka akācijas"), 1))(search_fuzzy(word("ukācijas"), 1))
    assertResult((List("aka akācijas"), 1))(search_fuzzy(word("akācijs"), 1))
    assertResult((List("aka akācijas"), 1))(search_fuzzy(word("akācijaz"), 1))
    assertResult((List("aka akācijas"), 1))(search_fuzzy(word("akācijass"), 1))
    assertResult((List("akls"), 1))(search_fuzzy(word("aklas"), 1))
    assertResult((List("akls"), 1))(search_fuzzy(word("kakls"), 1))
    assertResult((List("ventspils"), 0))(search_fuzzy(word("ventspils"), 1))
    assertResult((List("ventspils"), 1))(search_fuzzy(word("venspils"), 1))
    assertResult((Nil, 1))(search_fuzzy(word("vencpils"), 1))
    assertResult((Nil, 1))(search_fuzzy(word("kaklas"), 1))

    assertResult((List("ventspils"), 1))(search_fuzzy(word("venspils"), 1))
    assertResult((List("ventspils"), 1))(search_fuzzy(word("ventpils"), 1))
    assertResult((Nil, 1))(search_fuzzy(word("venpils"), 1))
    assertResult((List("kazdanga"), 1))(search_fuzzy(word("bazdanga"), 1))
    assertResult((List("ventspils"), 1))(search_fuzzy(word("bentspils"), 1))
    assertResult((List("kazdanga"), 1))(search_fuzzy(word("vazdanga"), 1))
    assertResult((List("ventspils"), 1))(search_fuzzy(word("kentspils"), 1))

    //fuzzy search, edit distance 2
    assertResult((List("akls"), 2))(search_fuzzy(word("akliss"), 2))
    assertResult((List("akls"), 2))(search_fuzzy(word("kaklas"), 2))
    assertResult((List("akls"), 2))(search_fuzzy(word("kikls"), 2))
    assertResult((List("akls"), 2))(search_fuzzy(word("akliss"), 2))
    assertResult((Nil, 2))(search_fuzzy(word("kakliss"), 2))
    assertResult((List("ventspils"), 1))(search_fuzzy(word("ventpils"), 2))
    assertResult((List("ventspils"), 1))(search_fuzzy(word("venspils"), 2))
    assertResult((List("ventspils"), 2))(search_fuzzy(word("venpils"), 2))
    assertResult((List("ventspils"), 2))(search_fuzzy(word("vencpils"), 2))
  }
}
