// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "gtest/gtest.h"
#include "helper/AssembledContainer.h"

#include <vector>

typedef int type_t;
typedef std::vector<type_t> vector_t;
typedef AssembledContainer<vector_t, type_t> container_t;



class AssembledContainerTest : public ::testing::Test {
 protected:
  
  AssembledContainerTest() {
    v1 = {1, 2, 3, 4};
    v2 = {5, 6};
    v4 = {7, 8, 9};
    c.add(v1.begin(), v1.end());
    c.add(v2.begin(), v2.end());
    c.add(v3.begin(), v3.end());
    c.add(v4.begin(), v4.end());
  }

  container_t c;
  vector_t v1;
  vector_t v2;
  vector_t v3;
  vector_t v4;
};

size_t count(container_t &c) {
  size_t count = 0;
  auto it = c.begin();
  while (it != c.end()) {
    ++count;
    ++it;
  }
  return count;
}

TEST_F(AssembledContainerTest, empty_should_be_emtpy) {
  container_t empty_container;
  ASSERT_EQ(count(empty_container), 0);

  // add empty vector
  vector_t v;
  empty_container.add(v.begin(), v.end());

  ASSERT_EQ(count(empty_container), 0);
}

TEST_F(AssembledContainerTest, single_value_checks) {
  
  auto it = c.begin();

  ASSERT_TRUE(it != c.end());
  ASSERT_TRUE(++it != c.end());
  ASSERT_FALSE(it == c.end());
  ASSERT_TRUE(++it != c.end());
  ASSERT_FALSE(it == c.end());
  ASSERT_TRUE(++it != c.end());
  ASSERT_FALSE(it == c.end());
  ASSERT_TRUE(++it != c.end());
  ASSERT_FALSE(it == c.end());
  ASSERT_TRUE(++it != c.end());
  ASSERT_FALSE(it == c.end());
  ASSERT_TRUE(++it != c.end());
  ASSERT_FALSE(it == c.end());
  ASSERT_TRUE(++it != c.end());
  ASSERT_FALSE(it == c.end());
  ASSERT_TRUE(++it != c.end());
  ASSERT_FALSE(it == c.end());
  ASSERT_TRUE(++it == c.end());
  ASSERT_FALSE(it != c.end());

  ASSERT_EQ(count(c), 9);
  size_t i = 0;
  for (auto v : c) {
    ASSERT_EQ(++i, v);
  }
}

TEST_F(AssembledContainerTest, iterator_plus_minus) {
  auto it = c.begin();
  ASSERT_EQ(*it, 1);
  it = it + 2;
  ASSERT_EQ(*it, 3);
  it = it - 1; 
  ASSERT_EQ(*it, 2);
  it = it + 4; 
  ASSERT_EQ(*it, 6);
  it = it + 3; 
  ASSERT_EQ(*it, 9);
  it = it - 6; 
  ASSERT_EQ(*it, 3);

  auto it2 = c.end();
  it2 = it2 - 1; 
  ASSERT_EQ(*it2, 9);
  it2 = it2 - 5; 
  ASSERT_EQ(*it2, 4);
  it2 = it2 - 3; 
  ASSERT_EQ(*it2, 1);
}

TEST_F(AssembledContainerTest, forward_iterate) {
  auto it = c.begin();
  size_t i = 0;
  while (it != c.end()) {
    ASSERT_EQ(*it, ++i);
    ++it;
  }
  ASSERT_EQ(i, 9);
}

TEST_F(AssembledContainerTest, reverse_iterate) {
  auto it = c.end();
  size_t i = 10;
  size_t count = 0;
  while (it != c.begin()) {
    it = it - 1;
    ASSERT_EQ(*it, --i);
    ++count;
  }
  ASSERT_EQ(count, 9);
}

TEST_F(AssembledContainerTest, compare_iterators) {
  ASSERT_EQ(c.begin(), c.begin());
  ASSERT_EQ(c.end(), c.end());
  ASSERT_EQ(c.begin()+2, c.begin()+2);
  ASSERT_EQ(c.begin()+4, c.end()-5);

  ASSERT_NE(c.begin(), c.end());
  ASSERT_NE(c.begin()+2, c.end()-5);

  container_t empty_container;
  ASSERT_EQ(empty_container.begin(), empty_container.end());  
}

TEST_F(AssembledContainerTest, iterator_difference) {
  // TODO: distance checks
  // ASSERT_EQ(c.end() - c.begin(), 9);
  ASSERT_EQ(std::distance( c.begin(), c.end() ) , 9);
  ASSERT_EQ(c.size(), 9);
}


