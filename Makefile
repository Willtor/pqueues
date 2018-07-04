DEF ?= def
DEFGHI = defghi
SET_BENCH = set_bench
PRIORITY_BENCH = priority_bench

OPTLEVEL = -O3

DEFFLAGS = $(OPTLEVEL)
DEFLIBS = -lpthread

CC = clang
CFLAGS = $(OPTLEVEL) -mrtm

SETS =		\
	fhsl_lf.def	\
	fhsl_b.def	\
	fhsl_tx.def \
	def_bt_lf.def \
	def_bt_tx.def

PQUEUES = \
	fhsl_lf.def \
	sl_pqueue.def

	#pq_spray.def
DEFIFILES = $(SETS:.def=.defi) $(PQUEUES:.def=.defi)

STACKTRACK = atomics.c common.c htm.c skip-list.c stack-track.c
BT_LF = bt_lf.c

SET_SRC = $(SETS) $(STACKTRACK) $(BT_LF) set_bench.def
SET_DEF_OBJ = $(SET_SRC:.def=.o)
SET_OBJ = $(SET_DEF_OBJ:.c=.o)

PRIORITY_SRC = $(PQUEUES) priority_bench.def
PRIORITY_DEF_OBJ = $(PRIORITY_SRC:.def=.o)
PRIORITY_OBJ = $(PRIORITY_DEF_OBJ:.c=.o)

all: $(SET_BENCH) $(PRIORITY_BENCH)

$(SET_BENCH): $(SET_OBJ)
	$(DEF) -o $@ $(DEFFLAGS) $(DEFLIBS) $^

$(PRIORITY_BENCH): $(PRIORITY_OBJ)
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
