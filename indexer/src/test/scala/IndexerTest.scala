package lv.addresses.indexer

import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

class IndexerTest extends FunSuite {

  class TestAddressFinder(val addressFileName: String, val blackList: Set[String],
                          val houseCoordFile: String, val dbConfig: Option[DbConfig]) extends AddressFinder

  val finder = new TestAddressFinder(null, Set.empty, null, None)

  import spray.json._
  object IndexJsonProtocol extends DefaultJsonProtocol {
    implicit object NodeFormat extends RootJsonFormat[finder.MutableIndexNode] {
      override def write(obj: finder.MutableIndexNode): JsValue = {
        import obj._
        JsObject(Map(
          "word" -> Option(word).map(JsString(_)).getOrElse(JsNull),
          "codes" -> Option(codes).map(_.toVector.toJson).getOrElse(JsNull),
          "children" -> Option(children).map(_.map(write).toVector).map(JsArray(_)).getOrElse(JsNull)
        ))
      }

      override def read(json: JsValue): finder.MutableIndexNode = json match {
        case JsObject(fields) =>
          new finder.MutableIndexNode(
            fields("word").convertTo[String],
            ArrayBuffer.from(fields("codes").convertTo[Vector[Int]]),
            ArrayBuffer.from(fields("children") match { case JsArray(ch) => ch.map(read)})
          )
      }
    }
    implicit object IndexFormat extends RootJsonFormat[finder.MutableIndex] {
      override def write(obj: finder.MutableIndex): JsValue = {
        import obj._
        JsArray(Option(children).map(_.map(_.toJson).toVector).getOrElse(Vector()))
      }

      override def read(json: JsValue): finder.MutableIndex = json match {
        case JsArray(children) =>
          new finder.MutableIndex(ArrayBuffer.from(children.map(_.convertTo[finder.MutableIndexNode])))
      }
    }
  }

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

  test("index mutable node") {
    val node = List(
      "aknas",
      "akls",
      "ak ak",
      "aknīste",
      "ak aknīste"
    )
    .zipWithIndex
    .foldLeft(new finder.MutableIndex(ArrayBuffer())) { (node, addrWithIdx) =>
      val (addr, idx) = addrWithIdx
      finder.extractWords(addr).foreach(node.updateChildren(_, idx))
      node
    }

    val expectedResult =
      """
        [{
        |  "word": "2*a",
        |  "codes": [2, 4],
        |  "children": null
        |}, {
        |  "word": "2*ak",
        |  "codes": [2, 4],
        |  "children": null
        |}, {
        |  "word": "a",
        |  "codes": [0, 1, 2, 3, 4],
        |  "children": [{
        |    "word": "k",
        |    "codes": [0, 1, 2, 3, 4],
        |    "children": [{
        |      "word": "l",
        |      "codes": [1],
        |      "children": [{
        |        "word": "s",
        |        "codes": [1],
        |        "children": null
        |      }]
        |    }, {
        |      "word": "n",
        |      "codes": [0, 3, 4],
        |      "children": [{
        |        "word": "a",
        |        "codes": [0],
        |        "children": [{
        |          "word": "s",
        |          "codes": [0],
        |          "children": null
        |        }]
        |      }, {
        |        "word": "i",
        |        "codes": [3, 4],
        |        "children": [{
        |          "word": "s",
        |          "codes": [3, 4],
        |          "children": [{
        |            "word": "t",
        |            "codes": [3, 4],
        |            "children": [{
        |              "word": "e",
        |              "codes": [3, 4],
        |              "children": null
        |            }]
        |          }]
        |        }]
        |      }]
        |    }]
        |  }]
        |}]
        |""".stripMargin.parseJson
    import IndexJsonProtocol._
    //println(node.toJson.prettyPrint)
    assertResult(expectedResult)(node.toJson)
  }
}
