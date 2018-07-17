DEF ?= def
DEFGHI = defghi
SET_BENCH = set_bench
PRIORITY_BENCH = priority_bench

OPTLEVEL = -O3

DEFFLAGS = $(OPTLEVEL) --ftransactions=hardware
DEFLIBS = -lpthread -lm

CC = clang
CFLAGS = $(OPTLEVEL) -mrtm

SETS = \
	fhsl_lf.def	\
	fhsl_b.def	\
	fhsl_tx.def \
	bt_lf.def \
	bt_tx.def

PQUEUES = \
	fhsl_lf.def \
	fhsl_tx.def \
	shavit_lotan_pqueue.def \
	spray_pqueue.def \
	linden_jonsson_pqueue.def

C_PQS = c_fhsl_lf.c c_shavit_lotan_pqueue.c c_spray_pqueue.c c_linden_jonsson_pqueue.c

DEFIFILES = $(SETS:.def=.defi) $(PQUEUES:.def=.defi)

STACKTRACK = atomics.c common.c htm.c skip-list.c stack-track.c
C_BT_LF = c_bt_lf.c

SET_SRC = $(SETS) $(STACKTRACK) $(C_BT_LF) $(C_PQS) set_bench.def
SET_DEF_OBJ = $(SET_SRC:.def=.o)
SET_OBJ = $(SET_DEF_OBJ:.c=.o)

PRIORITY_SRC = $(PQUEUES) $(C_PQS) priority_bench.def
PRIORITY_DEF_OBJ = $(PRIORITY_SRC:.def=.o)
PRIORITY_OBJ = $(PRIORITY_DEF_OBJ:.c=.o)

all: $(SET_BENCH) $(PRIORITY_BENCH)

$(SET_BENCH): $(SET_OBJ)
	$(DEF) -o $@ $(DEFFLAGS) $(DEFLIBS) $^

$(PRIORITY_BENCH): $(PRIORITY_OBJ)
	$(DEF) -o $@ $(DEFFLAGS) $(DEFLIBS) $^

clean:
	rm -f $(SET_BENCH) $(PRIORITY_BENCH) *.defi *.o

set_bench.o: $(DEFIFILES)

priority_bench.o: $(DEFIFILES)

%.o: %.def
	$(DEF) -o $@ $(DEFFLAGS) -c $<

%.o: stacktrack/%.c
	$(CC) -o $@ $(CFLAGS) -c $<

%.defi: %.def
	$(DEFGHI) $<
