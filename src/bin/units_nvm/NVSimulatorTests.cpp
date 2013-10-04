#ifdef PERSISTENCY_NVRAM

// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "testing/test.h"
#include "io/NVSimulator.h"

#include <algorithm>
#include <time.h>
#include <sys/timeb.h>

namespace hyrise {
namespace storage {

class NVSimulatorTests : public ::hyrise::Test {
};

// see NVSimulator for documentation

constexpr int goal = 150;

TEST_F(NVSimulatorTests, delay_rdtscp_measure_rdtscp) {
  int uinaccuracy = 0, samples = 0, inaccuracy = 0, sum = 0;
  ndelay_rdtscp(0); // init static variable
  for(int i = 0; i < 1000; i++) {
    asm volatile("nop" ::: "memory");
    volatile int64_t start = rdtscp();
    asm volatile("nop" ::: "memory");
    volatile int64_t lap = rdtscp();
    ndelay_rdtscp(goal);
    volatile int64_t end = rdtscp();
    asm volatile("nop" ::: "memory");

    uint64_t realns = (end-start-2*(lap-start))*1000000000/((uint64_t)cpufreq_get_freq_kernel(sched_getcpu()) * 1000);

    if(abs(realns-goal) > 100) continue; // we were preempted
    sum += realns;
    uinaccuracy += abs(realns-goal);
    inaccuracy += realns-goal;
    samples++;
  }

  ASSERT_TRUE(uinaccuracy / samples < 20) << "When verifying with rdtscp, the rdtscp delay method had an average absolute inaccuracy of " << (uinaccuracy / samples) << " per sample with a arithmetic mean inaccuracy of " << (inaccuracy / samples) << ".";
  ASSERT_TRUE(samples > 950);
}

TEST_F(NVSimulatorTests, delay_rdtscp_measure_rt) {
  int uinaccuracy = 0, samples = 0, inaccuracy = 0, sum = 0;
  ndelay_rdtscp(0); // init static variable
  for(int i = 0; i < 1000; i++) {
    asm volatile("nop" ::: "memory");
    volatile int64_t start = realtime();
    asm volatile("nop" ::: "memory");
    volatile int64_t lap = realtime();
    ndelay_rdtscp(goal);
    volatile int64_t end = realtime();
    asm volatile("nop" ::: "memory");

    uint64_t realns = (end-start-2*(lap-start));

    if(abs(realns-goal) > 100) continue; // we were preempted
    sum += realns;
    uinaccuracy += abs(realns-goal);
    inaccuracy += realns-goal;
    samples++;
  }

  ASSERT_TRUE(uinaccuracy / samples < 20) << "When verifying with the realtime clock, the rdtscp delay method had an average absolute inaccuracy of " << (uinaccuracy / samples) << " per sample with a arithmetic mean inaccuracy of " << (inaccuracy / samples) << ".";
  ASSERT_TRUE(samples > 950);
}

TEST_F(NVSimulatorTests, delay_rt_measure_rdtscp) {
  int uinaccuracy = 0, samples = 0, inaccuracy = 0, sum = 0;
  ndelay_rdtscp(0); // init static variable
  for(int i = 0; i < 1000; i++) {
    asm volatile("nop" ::: "memory");
    volatile int64_t start = rdtscp();
    asm volatile("nop" ::: "memory");
    volatile int64_t lap = rdtscp();
    ndelay_rt(goal);
    volatile int64_t end = rdtscp();
    asm volatile("nop" ::: "memory");

    uint64_t realns = (end-start-2*(lap-start))*1000000000/((uint64_t)cpufreq_get_freq_kernel(sched_getcpu()) * 1000);

    if(abs(realns-goal) > 100) continue; // we were preempted
    sum += realns;
    uinaccuracy += abs(realns-goal);
    inaccuracy += realns-goal;
    samples++;
  }

  ASSERT_TRUE(uinaccuracy / samples < 50) << "When verifying with rdtscp, the rt delay method had an average absolute inaccuracy of " << (uinaccuracy / samples) << " per sample with a arithmetic mean inaccuracy of " << (inaccuracy / samples) << ".";
  ASSERT_TRUE(samples > 950);
}

TEST_F(NVSimulatorTests, delay_rt_measure_rt) {
  int uinaccuracy = 0, samples = 0, inaccuracy = 0, sum = 0;
  ndelay_rdtscp(0); // init static variable
  for(int i = 0; i < 1000; i++) {
    asm volatile("nop" ::: "memory");
    volatile int64_t start = realtime();
    asm volatile("nop" ::: "memory");
    volatile int64_t lap = realtime();
    ndelay_rt(goal);
    volatile int64_t end = realtime();
    asm volatile("nop" ::: "memory");

    uint64_t realns = (end-start-2*(lap-start));

    if(abs(realns-goal) > 100) continue; // we were preempted
    sum += realns;
    uinaccuracy += abs(realns-goal);
    inaccuracy += realns-goal;
    samples++;
  }

  ASSERT_TRUE(uinaccuracy / samples < 50) << "When verifying with the realtime clock, the rt delay method had an average absolute inaccuracy of " << (uinaccuracy / samples) << " per sample with a arithmetic mean inaccuracy of " << (inaccuracy / samples) << ".";
  ASSERT_TRUE(samples > 950);
}

}
}

#endif

