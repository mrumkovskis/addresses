package lv.addresses.indexer

trait SpatialIndexer { this: AddressFinder =>

  import scala.collection.mutable.SortedSet

  case class Node(code: Int, left: Node, right: Node)

  protected var _spatialIndex: Node = null

  class Search(val limit: Int) {
    private val realLimit = Math.min(limit, 20)
    private val nearest = SortedSet[(AddrObj, BigDecimal)]()(new Ordering[(AddrObj, BigDecimal)]  {
      def compare(a: (AddrObj, BigDecimal), b: (AddrObj, BigDecimal)) =
        if (a._2 < b._2) -1 else if (a._2 > b._2) 1 else 0
    })
    private def dist(px: BigDecimal, py: BigDecimal, ax: BigDecimal, ay: BigDecimal) =
      (px - ax).pow(2) + (py - ay).pow(2)

    def searchNearest(coordX: BigDecimal, coordY: BigDecimal) = {
      def search(node: Node, depth: Int = 0): AddrObj = if (node == null) null else {
        import node._
        val curr_addr = addressMap(code)
        def closest(addr: AddrObj, new_addr: AddrObj) = if (new_addr == null) {
          register_nearest(addr, null)
          addr
        } else {
          val nd = dist(coordX, coordY, new_addr.coordX, new_addr.coordY)
          val d = dist(coordX, coordY, addr.coordX, addr.coordY)
          val (ra, rd) = if (nd < d) (new_addr, nd) else (addr, d)
          register_nearest(ra, rd)
          ra
        }
        def register_nearest(addr: AddrObj, d: BigDecimal) = {
          nearest += (addr -> (if (d == null) dist(coordX, coordY, addr.coordX, addr.coordY) else d))
          if (nearest.size > realLimit) nearest.lastOption.foreach(nearest -= _)
        }
        def check_x_splitting_pane(addr: AddrObj) =
          dist(coordX, coordY, addr.coordX, addr.coordY) >= dist(coordX, 0, curr_addr.coordX, 0)
        def check_y_splitting_pane(addr: AddrObj) =
          dist(coordX, coordY, addr.coordX, addr.coordY) >= dist(0, coordY, 0, curr_addr.coordY)
        def traverse(left: Node, right: Node, check_splitting_pane_cross: AddrObj => Boolean,
            start_with_left: Boolean) = {
          val (first, second) = if (start_with_left) (left, right) else (right, left)
          val curr_best_addr = closest(curr_addr, search(first, depth + 1))
          if (check_splitting_pane_cross(curr_best_addr)) closest(curr_best_addr, search(second, depth + 1))
          else curr_best_addr
        }
        if (depth % 2 == 0) traverse(left, right, check_x_splitting_pane, coordX <= curr_addr.coordX) //x axis
        else traverse(left, right, check_y_splitting_pane, coordY <= curr_addr.coordY) //y axis
      }
      search(_spatialIndex)
      nearest.toList
    }

    def searchNearestFullScan(coordX: BigDecimal, coordY: BigDecimal) = {
      addressMap.foreach { case (c, o) =>
        if (o.coordX != null && o.coordY != null) {
          nearest += (o -> dist(coordX, coordY, o.coordX, o.coordY))
          if (nearest.size > realLimit) nearest.lastOption.foreach(nearest -= _)
        }
      }
      nearest.toList
    }
  }

  def spatialIndex(addressMap: Map[Int, AddrObj]) = {
    val start = System.currentTimeMillis
    var nodeCount = 0
    def kdtree(addresses: Seq[Int], depth: Int = 0): Node = addresses match {
      case Seq() => null
      case Seq(c) =>
        nodeCount += 1
        Node(c, null, null)
      case _ =>
        val axis = depth % 2
        val sorted = addresses.sortBy { c =>
          val a = addressMap(c)
          if (axis == 0) a.coordX else a.coordY
        }
        val median = sorted.size / 2
        nodeCount += 1
        Node(
          sorted(median),
          kdtree(sorted.view(0, median), depth + 1),
          kdtree(sorted.view(median, sorted.size), depth + 1)
        )
    }

    println("Creating spatial index ... ")
    _spatialIndex = kdtree(addressMap
      .keysIterator
      .filter(c => addressMap
        .get(c)
        .exists { a => a.coordX != null && a.coordY != null })
      .toIndexedSeq)
    println(s"Spatial index created ($nodeCount addresses indexed) in ${System.currentTimeMillis - start}ms")
  }
}
