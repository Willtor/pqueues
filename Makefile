DEF ?= def
DEFGHI = defghi
TARGET = pqueue_bench

OPTLEVEL = -O3

DEFFLAGS = $(OPTLEVEL)
DEFLIBS = -lpthread

CC = clang
CFLAGS = $(OPTLEVEL) -mrtm

DATASTRUCTS = \
  fhsl_lf.def \
  fhsl_b.def
DEFIFILES = $(DATASTRUCTS:.def=.defi)

STACKTRACK = atomics.c common.c htm.c skip-list.c stack-track.c

SRC = $(DATASTRUCTS) $(STACKTRACK) bench.def
OBJ1 = $(SRC:.def=.o)
OBJ = $(OBJ1:.c=.o)

all: $(TARGET)

$(TARGET): $(OBJ)
	$(DEF) -o $@ $(DEFFLAGS) $(DEFLIBS) $^

clean:
	rm -f $(TARGET) *.defi *.o

bench.o: $(DEFIFILES)

%.o: %.def
	$(DEF) -o $@ $(DEFFLAGS) -c $<

%.o: stacktrack/%.c
	$(CC) -o $@ $(CFLAGS) -c $<

%.defi: %.def
	$(DEFGHI) $<
