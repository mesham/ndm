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

all: example_p2p example_bcast example_reduce example_barrier example_groups mandelbrot mandelbrot_sep

example_p2p: src/example_p2p.o
	$(CC) -o example_p2p src/example_p2p.o $(LFLAGS)

example_bcast: src/example_bcast.o
	$(CC) -o example_bcast src/example_bcast.o $(LFLAGS)

example_reduce: src/example_reduce.o
	$(CC) -o example_reduce src/example_reduce.o $(LFLAGS)

example_barrier: src/example_barrier.o
	$(CC) -o example_barrier src/example_barrier.o $(LFLAGS)

example_groups: src/example_groups.o
	$(CC) -o example_groups src/example_groups.o $(LFLAGS)

mandelbrot: src/mandelbrot.o
	$(CC) -o mandelbrot src/mandelbrot.o $(LFLAGS) -lm

mandelbrot_sep: src/mandelbrot_sep.o
	$(CC) -o mandelbrot_sep src/mandelbrot_sep.o $(LFLAGS) -lm

.PHONEY: clean
clean:
	$(rm) src/*.o mandelbrot mandelbrot_sep example_barrier example_reduce example_bcast example_p2p
