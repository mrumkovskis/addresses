package lv.addresses.loader

import com.typesafe.scalalogging.Logger
import lv.addresses.indexer.{AddrObj, Addresses}
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.jdk.CollectionConverters.EnumerationHasAsScala
import scala.util.Using

object OpenDataLoader {
  import Loader._
  def loadAddresses(addressFile: String, addressHistoryFile: String): Addresses = {
    import lv.addresses.index.Index.normalize
    val structure = Map(
      "AW_PILSETA.CSV" -> Map(
        "code" -> 0,
        "typ" -> 1,
        "name" -> 2,
        "super_code" -> 3,
        "atvk" -> 12,
        "exists" -> 7,
      ),
      "AW_NOVADS.CSV" -> Map(
        "code" -> 0,
        "typ" -> 1,
        "name" -> 2,
        "super_code" -> 3,
        "atvk" -> 12,
        "exists" -> 7,
      ),
      "AW_PAGASTS.CSV" -> Map(
        "code" -> 0,
        "typ" -> 1,
        "name" -> 2,
        "super_code" -> 3,
        "atvk" -> 12,
        "exists" -> 7,
      ),
      "AW_CIEMS.CSV" -> Map(
        "code" -> 0,
        "typ" -> 1,
        "name" -> 2,
        "super_code" -> 3,
        "exists" -> 7,
      ),
      "AW_IELA.CSV" -> Map(
        "code" -> 0,
        "typ" -> 1,
        "name" -> 2,
        "super_code" -> 3,
        "exists" -> 7,
      ),
      "AW_EKA.CSV" -> Map(
        "code" -> 0,
        "typ" -> 1,
        "name" -> 7,
        "super_code" -> 5,
        "zip_code" -> 9,
        "koord_x" -> 17,
        "koord_y" -> 18,
        "exists" -> 2,
      ),
      "AW_DZIV.CSV" -> Map(
        "code" -> 0,
        "typ" -> 1,
        "name" -> 7,
        "super_code" -> 5,
        "exists" -> 2,
      ),
      "AW_PILSETA_HIS.CSV" -> Map(
        "code" -> 0,
        "name" -> 5,
      ),
      "AW_NOVADS_HIS.CSV" -> Map(
        "code" -> 0,
        "name" -> 5,
      ),
      "AW_PAGASTS_HIS.CSV" -> Map(
        "code" -> 0,
        "name" -> 5,
      ),
      "AW_CIEMS_HIS.CSV" -> Map(
        "code" -> 0,
        "name" -> 5,
      ),
      "AW_IELA_HIS.CSV" -> Map(
        "code" -> 0,
        "name" -> 5,
      ),
      "AW_EKA_HIS.CSV" -> Map(
        "code" -> 0,
        "name" -> 6,
      ),
      "AW_DZIV_HIS.CSV" -> Map(
        "code" -> 0,
        "name" -> 5,
      ),
    )

    def convertAddress(struct: Map[String, Int], line: Array[String]) = {
      if (line(struct("exists")) == "EKS")
        AddrObj(
          code      = line(struct("code")).toInt,
          typ       = line(struct("typ")).toInt,
          name      = line(struct("name")),
          superCode = line(struct("super_code")).toInt,
          zipCode   = struct.get("zip_code").map(line).orNull,
          words     = normalize(line(struct("name"))).toVector,
          coordX    = struct.get("koord_x").map(i => BigDecimal(line(i))).orNull,
          coordY    = struct.get("koord_y").map(i => BigDecimal(line(i))).orNull,
          atvk      = struct.get("atvk").map(line).orNull,
        )
      else null
    }

    def convertHistory(struct: Map[String, Int], line: Array[String]) = {
      line(struct("code")).toInt -> line(struct("name"))
    }

    def processFile[T](file: String, lineFun: (Map[String, Int], Array[String]) => T) =
      Using(new java.util.zip.ZipFile(file)) { af =>
        af.entries().asScala
          .flatMap { zipEntry =>
            logger.info(s"loading file $zipEntry")
            Source.fromInputStream(af.getInputStream(zipEntry), "Cp1257")
              .getLines()
              .drop(1)  // drop header row with column names
              .map { row =>
                val l =
                  row
                    .split(";")
                    .map(_.drop(1).dropRight(1)) // remove enclosing # from values
                lineFun(structure(zipEntry.getName), l)
              }
          }
      }

    logger.info(s"Loading addresses from file $addressFile ...")
    val addrObjs =
      processFile(addressFile, convertAddress)
        .get
        .map (o => o.code -> o)
        .toMap

    val addrHistory =
      processFile(addressHistoryFile, convertHistory)
        .get
        .foldLeft(scala.collection.mutable.Map[Int, List[String]]()) { case (res, (code, name)) =>
          res.addOne(code, name :: res.getOrElse(code, Nil))
        }
        .toMap

    Addresses(addrObjs, addrHistory)
  }
}

object Loader {
  private [loader] val logger = Logger(LoggerFactory.getLogger("lv.addresses.loader"))
}
