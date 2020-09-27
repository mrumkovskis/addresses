.SILENT:

all: compile

compile: 
	fsc  -opt:l:inline -opt-inline-from:** -opt:simplify-jumps -opt:compact-locals -deprecation -d . src/*.scala src/migration/*.scala

jar: compile
	fastjar -cvmf manifest.mf ./vzd-receive.jar lv/uniso/* lv/addresses/* &&\
	echo vzd-receive.jar created.
