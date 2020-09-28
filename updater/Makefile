.SILENT:

SCALA := $(if $(shell which fsc),     fsc,     scalac)
JAR   := $(if $(shell which fastjar), fastjar, jar)

all: compile

compile: 
	${SCALA}  -opt:l:inline '-opt-inline-from:**' -opt:simplify-jumps -opt:compact-locals -deprecation -d . src/*.scala src/migration/*.scala

jar: compile
	${JAR} -cvmf manifest.mf ./vzd-receive.jar lv/uniso/* lv/addresses/* &&\
	echo vzd-receive.jar created.
