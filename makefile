CC       = mpic++
# compiling flags here
CFLAGS   = -fPIC -O3

LFLAGS   =

# change these to set the proper directories where each files shoould be
SRCDIR   = src
OBJDIR   = build

SOURCES  := $(wildcard $(SRCDIR)/*.cpp)
INCLUDES := $(wildcard $(SRCDIR)/*.h)
OBJECTS  := $(SOURCES:$(SRCDIR)/%.cpp=$(OBJDIR)/%.o)
rm       = rm -f

ndm: $(OBJECTS)
	$(CC) -shared -o libndm.so $(OBJECTS) $(LFLAGS)

$(OBJECTS): $(OBJDIR)/%.o : $(SRCDIR)/%.cpp
	$(CC) $(CFLAGS) -c $< -o $@

.PHONEY: clean
clean:
	$(rm) $(OBJECTS)	
	$(rm) libndm.so
