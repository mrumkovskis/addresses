# VZD adrešu reģistra importētājs

Integrēts ar pārējo adrešu reģistra aplikāciju, buildojas ar sbt.

## Builds

    % sbt clean assembly
    ... target/scala-2.13/addresses-assembly-2.0.jar


## DB struktūra

Importētājs pats izveidos nepieciešamās tabulas ar Updater.scala norādīto struktūru, ja tās netiks atrastas.

Referencei, tabulu izveides skripts ir atrodams arī failā `db/create.sql`, bet tā manuāla laišana nav nepieciešama:

## Iestatījumi

Iestatījumi tiek ņemti no `application.conf`:

    VZD {
      driver = oracle.jdbc.OracleDriver
      url =  jdbc:oracle:thin:... # localhost:1630 VZD tunelis
      user = vraa_amk_izstr
      password =
    }

    db {
      driver = org.postgresql.Driver
      url = "jdbc:postgresql://127.0.0.1:5432/adreses?rewriteBatchedStatements=true"
      user = postgres
      password =
    }


Augstāk norādītas ir noklusētās vērtības. Tehniski, visdrīzāk, pietiek norādīt vien paroles.


Alternatīvi, iestatījumus var norādīt arī kā komandrindas parametrus, kurus var uzzināt ar `--help` komandu.


## Darbināšana

Sinhronizācija tiek iedarbināta, rezultējošajam jaram kā pirmo parametru norādot "sync":

    % java -jar addresses-assembly-2.0.jar sync --help

    Updater retrieves up-to-date address data from VZD address register.

    Usage:
      vzd-receive [OPTION]... [table_to_migrate table_to_migrate ...]

      (... snip ...)

    Transfer options:
      --lock LOCKFILE               use alternate lockfile
                                    (default: ${default_opts("lockfile")})
      --verify                      verify data integrity (ids only)


    Supported migration tables:
      arg_adrese, arg_adrese_arh, art_vieta, art_nlieta, art_eka_geo

    Examples:

      Ordinary sync:
      % ./vzd-receive

      Update arg_adrese only:
      % ./vzd-receive arg_adrese

