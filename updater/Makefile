.SILENT:

SCALA := $(if $(shell which fsc),     fsc,     scalac)
JAR   := $(if $(shell which fastjar), fastjar, jar)

all: compile

compile: lib
	${SCALA}  -opt:l:inline '-opt-inline-from:**' -opt:simplify-jumps -opt:compact-locals -deprecation -d . src/*.scala src/migration/*.scala

jar: compile
	${JAR} -cvmf manifest.mf ./vzd-receive.jar lv/uniso/* lv/addresses/* &&\
	echo vzd-receive.jar created.

lib:
	mkdir lib
	curl https://www.datanucleus.org/downloads/maven2/oracle/ojdbc6/11.2.0.3/ojdbc6-11.2.0.3.jar --output lib/ojdbc6.jar
	curl https://jdbc.postgresql.org/download/postgresql-42.2.16.jar --output lib/postgresql.jar
	curl https://repo1.maven.org/maven2/org/scala-lang/scala-library/2.13.3/scala-library-2.13.3.jar --output lib/scala-library.jar
