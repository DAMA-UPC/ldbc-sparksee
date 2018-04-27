#include "gtest/gtest.h"

#include "tools/snbLoader.h"
#include "gdb/Sparksee.h"
#include "gdb/Database.h"
#include "gdb/Session.h"
#include "gdb/Graph.h"

namespace {

class LoaderTest : public ::testing::Test {
protected:
  // You can do set-up work for each test here.
  LoaderTest() {}

  // You can do clean-up work that doesn't throw exceptions here.
  virtual ~LoaderTest() {}

  // Code here will be called immediately after the constructor (right
  // before each test).
  virtual void SetUp() {
    std::string parseFile("load.txt");
    ASSERT_EQ(0, snb_loader::Parse(parseFile.c_str()))
        << "On SetUp: Failed to prepare the database";
  }

  // Code here will be called immediately after each test (right
  // before the destructor).
  virtual void TearDown() {}

  // Objects declared here can be used by all tests in the test case.
};

TEST_F(LoaderTest, schemaIsComplete) {}
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
