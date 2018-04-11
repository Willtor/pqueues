DEF = def
DEFGHI = defghi
TARGET = pqueue_bench

DEFFLAGS = -O3

DATASTRUCTS = fhsl_lf.def
SRC = $(DATASTRUCTS) bench.def
OBJ = $(SRC:.def=.o)

all: $(TARGET)

$(TARGET): $(SRC:.def=.o)
	$(DEF) -o $@ $(DEFFLAGS) $^

clean:
	rm -f $(TARGET) *.defi *.o

%.o: %.def %.defi
	$(DEF) -o $@ $(DEFFLAGS) -c $<

%.defi: %.def
	$(DEFGHI) $<
