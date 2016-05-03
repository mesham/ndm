CC       = mpicc
# compiling flags here
CFLAGS   = -O3

LFLAGS   = -L../ -lndm

# change these to set the proper directories where each files shoould be
SRCDIR   = src
OBJDIR   = build

rm       = rm -f

%.o: %.c
	$(CC) $(CFLAGS) -I../src/ -c $< -o $@ 

all: example_p2p example_bcast example_reduce example_barrier mandelbrot

example_p2p: src/example_p2p.o
	$(CC) -o example_p2p src/example_p2p.o $(LFLAGS)

example_bcast: src/example_bcast.o
	$(CC) -o example_bcast src/example_bcast.o $(LFLAGS)

example_reduce: src/example_reduce.o
	$(CC) -o example_reduce src/example_reduce.o $(LFLAGS)

example_barrier: src/example_barrier.o
	$(CC) -o example_barrier src/example_barrier.o $(LFLAGS)

mandelbrot: src/mandelbrot.o
	$(CC) -o mandelbrot src/mandelbrot.o $(LFLAGS) -lm

.PHONEY: clean
clean:
	$(rm) src/*.o mandelbrot example_barrier example_reduce example_bcast example_p2p
