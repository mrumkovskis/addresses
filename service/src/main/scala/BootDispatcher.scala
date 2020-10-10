package lv.addresses

object BootDispatcher extends scala.App {
  if (args.length == 0) {
    service.Boot.main(args)
  } else if (args(0) == "sync") {
    Updater.main(args.tail)
  } else {
    lv.addresses.service.Boot.main(args)
  }
}
