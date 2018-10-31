DEF ?= def
DEFGHI = defghi
SET_BENCH = set_bench
PRIORITY_BENCH = priority_bench

OPTLEVEL = -O3

DEFFLAGS = $(OPTLEVEL) --ftransactions=hardware
DEFLIBS = -lpthread -lm -lcpuinfo

CC = clang
CFLAGS = $(OPTLEVEL) -mrtm

DEF_SETS = \
	fhsl_lf.def \
	fhsl_b.def	\
	fhsl_tx.def \
	bt_lf.def \
	bt_tx.def

C_SETS = \
	c_fhsl_lf.c \
	c_fhsl_tx.c \
	c_bt_lf.c

DEF_PQUEUES = \
	fhsl_lf.def \
	fhsl_tx.def \
	sl_pq.def \
	spray_pq.def \
	spray_tx_pq.def \
	lj_pq.def \
	serial_btree.def \
	mq_locked_btree.def

C_PQUEUES = \
	c_sl_pq.c \
	c_spray_pq.c \
	c_spray_clean_pq.c \
	c_spray_pq_tx.c \
	c_lj_pq.c


DEFIFILES = $(DEF_SETS:.def=.defi) $(DEF_PQUEUES:.def=.defi)

STACKTRACK = atomics.c common.c htm.c skip-list.c stack-track.c

SET_SRC = $(DEF_SETS) $(STACKTRACK) $(C_SETS) utils.c elided_lock.c thread_pinner.c set_bench.def
SET_DEF_OBJ = $(SET_SRC:.def=.o)
SET_OBJ = $(SET_DEF_OBJ:.c=.o)

PRIORITY_SRC = $(DEF_PQUEUES) $(C_PQUEUES) $(DEF_SETS) $(C_SETS) utils.c elided_lock.c thread_pinner.c priority_bench.def
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
