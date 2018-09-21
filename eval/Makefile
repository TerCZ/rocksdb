include ../make_config.mk

ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += -DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif

ifneq ($(USE_RTTI), 1)
	CXXFLAGS += -fno-rtti
endif

.PHONY: clean librocksdb

all: m_bench

m_bench: m_bench.cpp util workload librocksdb
	$(CXX) $(CXXFLAGS) $@.cpp util.o workload.o ../librocksdb.a -o $@ -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS) && rm *.o

util: util.cpp util.h
	$(CXX) $(CXXFLAGS) $@.cpp -c -o $@.o -O2 -std=c++11 $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

workload: workload.cpp workload.h util
	$(CXX) $(CXXFLAGS) $@.cpp -c -o $@.o -I../include -O2 -std=c++11 $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

clean:
	rm -rf ./ycsb_a *.o

librocksdb:
	cd .. && $(MAKE) static_lib
