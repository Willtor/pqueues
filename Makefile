DEF ?= def
DEFGHI = defghi
SET_BENCH = set_bench
PRIORITY_BENCH = priority_bench
TARGETS = $(SET_BENCH) $(PRIORITY_BENCH)

OPTLEVEL = -O3

DEFFLAGS = $(OPTLEVEL)
DEFLIBS = -lpthread

CC = clang
CFLAGS = $(OPTLEVEL) -mrtm

DATASTRUCTS =		\
	fhsl_lf.def	\
	fhsl_b.def	\
	fhsl_tx.def \
	def_bt_lf.def \
	def_bt_tx.def

DEFIFILES = $(DATASTRUCTS:.def=.defi)

STACKTRACK = atomics.c common.c htm.c skip-list.c stack-track.c

BT_LF = bt_lf.c

SRC = $(DATASTRUCTS) $(STACKTRACK) $(BT_LF)
OBJ1 = $(SRC:.def=.o)
OBJ = $(OBJ1:.c=.o)

SET_BENCH_SRC = $(SET_BENCH:=.def)
PRIORITY_BENCH_SRC = $(PRIORITY_BENCH:=.def)

all: $(SET_BENCH) $(PRIORITY_BENCH)

$(SET_BENCH): $(SET_BENCH_SRC:.def=.o) $(OBJ)
	$(DEF) -o $@ $(DEFFLAGS) $(DEFLIBS) $^

$(PRIORITY_BENCH): $(PRIORITY_BENCH_SRC:.def=.o) $(OBJ)
	$(DEF) -o $@ $(DEFFLAGS) $(DEFLIBS) $^

clean:
	rm -f $(TARGET) *.defi *.o

set_bench.o: $(DEFIFILES)

priority_bench.o: $(DEFIFILES)

%.o: %.def
	$(DEF) -o $@ $(DEFFLAGS) -c $<

%.o: stacktrack/%.c
	$(CC) -o $@ $(CFLAGS) -c $<

%.defi: %.def
	$(DEFGHI) $<
