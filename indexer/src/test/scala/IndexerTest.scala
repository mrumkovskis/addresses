package lv.addresses.indexer

import org.scalatest.FunSuite

class IndexerTest extends FunSuite {

  class TestAddressFinder(val addressFileName: String, val blackList: Set[String],
                          val houseCoordFile: String, val dbConfig: Option[DbConfig]) extends AddressFinder

  test("word stat for indexer") {
    val finder = new TestAddressFinder(null, Set.empty, null, None)
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
    val finder = new TestAddressFinder(null, Set.empty, null, None)
    assertResult(Map("nov" -> 1, "Valles" -> 2, "pag" -> 1, "Vecumnieku" -> 1, "vidusskola" -> 1, "Valle" -> 3))(
      finder.wordStatForSearch(Array("Valles", "vidusskola", "Valle", "Valles", "pag", "Vecumnieku", "nov")))
  }
}
