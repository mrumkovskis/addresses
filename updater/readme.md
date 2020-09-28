# VZD adrešu reģistra importētājs

Šobrīd — nekā neintegrēts ar sbt vai pārējo aplikāciju, bet pilnīgi standalone jars.

Buildojas ar `sbt`, pakojas ar `sbt-assembly`.

## Builds

    % sbt clean assembly
    ... target/scala-2.13/updater-assembly-0.1.0-SNAPSHOT.jar


## DB struktūra

Importētājs pats izveidos nepieciešamās tabulas ar Updater.scala norādīto struktūru, ja tās netiks atrastas.

Referencei, tabulu izveides skripts ir atrodams arī failā `../db/create.sql`, bet tā manuāla laišana nav nepieciešama:


## Darbināšana

    % java -jar updater-assembly.jar --help
    % alternatīvi, sbt 'run --help'

    Updater retrieves up-to-date address data from VZD address register.

    Usage:
      vzd-receive [OPTION]... [table_to_migrate table_to_migrate ...]


    Destination connection options:
      --driver DRIVER               JDBC driver to connect to destination database
                                    (default: "org.postgresql.Driver")
      --connection CONNECTION       JDBC connection string for destination connection
                                    (default: "jdbc:postgresql://127.0.0.1:5432/adreses?rewriteBatchedStatements=true")
      --username USERNAME           username to connect to destination
                                    (default: "postgres")
      --password PASSWORD           connection password to destination

    VZD connection options:
      --vzd-driver DRIVER          JDBC driver to connect to VZD
                                    (default: "oracle.jdbc.OracleDriver")
      --vzd CONNECTION
      --vzd-connection CONNECTION  JDBC connection string for VZD connection
      --vzd-username USERNAME      username to connect to VZD
                                    (default: "vraa_amk_izstr")
      --vzd-password PASSWORD      connection password to VZD

    Transfer options:
      --lock LOCKFILE               use alternate lockfile
                                    (default: /tmp/addresses-vzd-receive.lock)
      --verify                      verify data integrity (ids only)

    Supported migration tables:
      arg_adrese, arg_adrese_arh, art_vieta, art_nlieta, art_eka_geo

    Examples:

      Ordinary sync:
      % ./vzd-receive

      Update arg_adrese only:
      % ./vzd-receive arg_adrese


      

## Konfigurācija

Konfigurācija tiek jaram nodota komandrindā. Noklusētie iestatījumi:

- local: postgres zem 127.0.0.1, datu bāze "adreses", lietotājs "postgres", parole tukša,
- vzd: oracle ar proxy zem 127.0.0.1:1630, lietotājs vraa_amk_izstr, parole tukša.

Tipiskajam lietojumam pietiek vien norādīt lietotāja `vraa_amk_izstr` paroli,

    % sbt 'run --vzd-password XXXX'


