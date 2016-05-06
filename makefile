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
rm       = rm -Rf

all: ndm

ndm: build_buildDir $(OBJECTS)
	$(CC) -shared -Wl,-soname,libndm.so -o libndm.so $(OBJECTS) $(LFLAGS)
	ar rcs libndm.a $(OBJECTS) $(LFLAGS)

build_buildDir:
	@mkdir -p $(OBJDIR)

$(OBJECTS): $(OBJDIR)/%.o : $(SRCDIR)/%.cpp
	$(CC) $(CFLAGS) -c $< -o $@

.PHONEY: clean
clean:
	$(rm) $(OBJDIR)	
	$(rm) libndm.so
