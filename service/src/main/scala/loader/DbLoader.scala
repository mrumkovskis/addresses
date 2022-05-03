package lv.addresses.service.loader

import lv.addresses.index.Index.normalize
import lv.addresses.indexer.{AddrObj, Addresses, Constants}
import org.tresql.{Query, Resources}

import java.sql.DriverManager
import scala.util.Using

object DbLoader {
  def loadAddresses(driver: String,
                    url: String, user: String,
                    password: String,
                    tresqlResources: Resources): Addresses = {
    import Loader._
    Class.forName(driver)
    Using(DriverManager.getConnection(url, user, password)) { conn =>
      implicit val res = tresqlResources withConn conn

      logger.info(s"Loading house coordinates")
      val houseCoords = {
        // koord_y - latitude
        // koord_x - longitude
        Query("art_eka_geo {vieta_cd, koord_x, koord_y}")
          .map(row => row.int("vieta_cd") ->
            (row.bigdecimal("koord_y") -> row.bigdecimal("koord_x")))
          .toMap
      }
      logger.info(s"House coordinates loaded: ${houseCoords.size} objects")

      logger.info("Loading address objects")
      val queries = List(
        "art_vieta [statuss = 'EKS' & tips_cd in ?] {kods, tips_cd, vkur_cd, nosaukums, atrib}",
        "art_nlieta [statuss = 'EKS' & tips_cd in ?] {kods, tips_cd, vkur_cd, nosaukums, atrib}",
        "art_dziv [statuss = 'EKS' & tips_cd in ?] {kods, tips_cd, vkur_cd, nosaukums, atrib}",
      )

      val addressObjs: Map[Int, AddrObj] =
        queries
          .map(Query(_, Constants.typeOrderMap.keys))
          .map {
            _.map { row =>
              val kods = row.long("kods").intValue()
              val (koordLat, koordLong) = houseCoords.getOrElse(kods, (null, null))
              val name = row.string("nosaukums")
              val tips = row.long("tips_cd").intValue
              val attr = row.string("atrib")
              def normalizeAttrib(types: Int*) = if (types.toSet(tips)) attr else null
              val zipCode = normalizeAttrib(Constants.NLT)
              val atvk = normalizeAttrib(Constants.PAG, Constants.PIL, Constants.NOV)
              val obj = AddrObj(kods, tips, name,
                row.long("vkur_cd").intValue, zipCode,
                normalize(name).toVector, koordLat, koordLong, atvk)
              kods -> obj
            }
          }
          .reduce(_ ++ _)
          .toMap
      logger.info(s"Addresses loaded: ${addressObjs.size} objects")

      logger.info("Loading address history")
      val history =
        Query("arg_adrese_arh {adr_cd, string_agg(std, ?)} (adr_cd)", "\n")
          .map(row => row.int(0) -> row.string(1).split("\n").toList.distinct)
          .toMap
      logger.info(s"Address history loaded: ${history.size} objects")

      Addresses(updateIsLeafFlag(addressObjs), history)
    }.get
  }
}
