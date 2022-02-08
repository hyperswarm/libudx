CFLAGS += -Iinclude

libudx.a: \
	src/cirbuf.o \
	src/fifo.o \
	src/udx.o \
	src/utils.o
	rm -f $@
	$(AR) rcs $@ $^

src/cirbuf.o: include/udx/cirbuf.h
src/fifo.o: include/udx/fifo.h
src/udx.o: include/udx.h
src/utils.o: src/utils.h