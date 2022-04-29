package lv.addresses.service.loader

import com.typesafe.scalalogging.Logger
import lv.addresses.index.Index.binarySearch
import lv.addresses.indexer.AddrObj
import org.slf4j.LoggerFactory

object Loader {
  private [loader] val logger = Logger(LoggerFactory.getLogger("lv.addresses.loader"))
  import scala.collection.mutable.{ArrayBuffer => AB}

  case class AddrObjNode(code: Int, children: AB[AddrObjNode] = AB())
  case class AddrObjTree(children: AB[AddrObjNode] = AB()) {
    def add(codes: List[Int]): Unit = {
      def add(codes: List[Int], children: AB[AddrObjNode]): Unit = codes match {
        case Nil =>
        case code :: rest =>
          val idx = binarySearch[AddrObjNode, Int](children, code, _.code, _ - _)
          if (idx < 0) {
            val node = AddrObjNode(code)
            children.insert(-(idx + 1), node)
            add(rest, node.children)
          } else {
            add(rest, children(idx).children)
          }
      }
      add(codes, children)
    }
    def isLeaf(codes: List[Int]): Boolean = {
      def isLeaf(codes: List[Int], children: AB[AddrObjNode]): Boolean = codes match {
        case Nil => children.isEmpty
        case code :: rest =>
          val idx = binarySearch[AddrObjNode, Int](children, code, _.code, _ - _)
          idx >= 0 && isLeaf(rest, children(idx).children)
      }
      isLeaf(codes, children)
    }
  }

  def updateIsLeafFlag(addresses: Map[Int, AddrObj]): Map[Int, AddrObj] = {
    logger.info("Setting leaf object marker...")
    val addressTree = AddrObjTree()
    def codes(ao: AddrObj) = ao.foldLeft(addresses)(List[Int]())((c, o) => o.code :: c)
    addresses.foreach { case (_, ao) =>
      //update index
      addressTree.add(codes(ao))
    }
    val res = addresses.map { case (code, ao) =>
      //update isLeaf flag
      (code, if (addressTree.isLeaf(codes(ao))) ao else ao.copy(isLeaf = false))
    }
    logger.info("Leaf object marker set.")
    res
  }
}
