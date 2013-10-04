#ifndef SRC_LIB_IO_NVSIMULATOR_H
#define SRC_LIB_IO_NVSIMULATOR_H

#define DELAY_METHOD ndelay_rdtscp

/*
	DELAY HOWTO
	There are two different methods to stall the CPU for n nanoseconds,
	ndelay_rdtscp and ndelay_rt.

	ndelay_rdtscp
		This method makes use of the CPU's time stamp counter. Because RDTSC
		is not a synchronized operation, RDTSCP is used. This only works if
		the CPU frequency is constant, i.e., technologies such as SpeedStep
		are disabled

	ndelay_rt
		This method uses CLOCK_REALTIME.

	These approaches have certain tradeoffs:
	Resolution
		The precision of the delay function is influenced by how long the timing
		function (i.e., rdtscp and realtime) take to execute. As realtime takes
		longer, the resolution of ndelay_rt is lower than that of ndelay_rdtscp.

		The table below gives the WORST average error (in ns) encountered for
		every combination of delay algorithm and verification algorithm.

		           measure algo| rdtscp | rt
		delay algo             |
		-----------------------+--------+---
		ndelay_rdtscp          |   8    | 9
		ndelay_rt              |   41   | 14

	Minimum Delay
		For a similar reason, the two approaches have a minimum delay:

		           measure algo| rdtscp | rt
		delay algo             |
		-----------------------+--------+---
		ndelay_rdtscp          |   41   | 38
		ndelay_rt              |   99   | 74

	CPU switch tolerance
		So... why would you use ndelay_rt anyway? The time stamp counter TSC
		is not synchronized between CPUs. Thus, if you expect context switches
		to happen during the delay, and you are not pinned to a single CPU,
		you probably want to use ndelay_rt.

	Testing
		For testing, all combinations are executed with a reasonable delay
		(currently 150). The expected average error is below 20 which is the
		pass condition for the test.
*/

#include <cpufreq.h>
#include <stdint.h>
#include <sched.h>
#include <stdio.h>
#include <assert.h>

__attribute__((always_inline)) inline uint64_t rdtscp() {
    uint64_t rax, rdx, aux;
    asm volatile ("rdtscp\n" : "=a" (rax), "=d" (rdx), "=c" (aux) : :);
    return (rdx << 32) + rax;
}

__attribute__((always_inline)) inline uint64_t realtime() {
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000000000LL + (uint64_t)ts.tv_nsec;
}

__attribute__((noinline, unused)) static void ndelay_rt(const uint64_t nsecs) {
	// do not inline this. it will break the timing.
	asm volatile(".align 32":::"memory");
	uint64_t target = realtime() + nsecs - 70;
	while(realtime() < target) {asm volatile("nop":::"memory");};
}

__attribute__((noinline, unused)) static void ndelay_rdtscp(const uint64_t nsecs) {
	// do not inline this. it will break the timing.
	asm volatile(".align 32":::"memory");
	asm volatile("nop");
	static uint64_t freq = (uint64_t)cpufreq_get_freq_kernel(sched_getcpu()) * 1000;
	// register uint64_t target asm("a5") = rdtscp() + (nsecs * freq / 1000000000)
		// this would make sure that we only operate on registers
	uint64_t target = rdtscp() + (nsecs * freq / 1000000000) - 69;
		// the subtracted clock ticks are spent outside of the loop
	while(rdtscp() < target) {asm volatile("nop":::"memory");}
}

class NVSimulator {
public:
	static inline __attribute__((always_inline)) void persist() {
		DELAY_METHOD(NVSIMULATOR_FLUSH_NS);
	};

	static inline __attribute__((always_inline)) void read() {
		DELAY_METHOD(NVSIMULATOR_READ_NS);
	};

	static inline __attribute__((always_inline)) void write() {
		DELAY_METHOD(NVSIMULATOR_WRITE_NS);
	};
};

#endif