
#include "tools/snbLoader.h"
#include "gdb/Sparksee.h"
#include "gdb/Database.h"
#include "gdb/Session.h"
#include "gdb/Graph.h"
#include <utils/Utils.h>
#include <sys/time.h>
#include <stdio.h>

namespace gdb = sparksee::gdb;
namespace utils = sparksee::utils;


void fill_objects( gdb::Objects* obj, int size, int offset, int stride, int block_size, int block_stride ) {
  int num_blocks = size / block_size;
  for(int j = 0; j < num_blocks; ++j) {
    for(int i = 0; i < block_size; ++i) {
      obj->Add(offset + j*block_stride + i*stride);
    }
  }
}

int main(int argc, char **argv) {

  gdb::SparkseeConfig cfg;
  utils::SparkseePtr sparksee(new gdb::Sparksee(cfg));
  std::string filename("./test.gdb");
  std::wstring wfilename(filename.begin(), filename.end());
  gdb::Database* database = sparksee->Create(wfilename, L"test");
  utils::SessionPtr session(database->NewSession());
  utils::GraphPtr graph(session->GetGraph());
  utils::ObjectsPtr objectsB(session->NewObjects());

  int step = 10;
  int initial_size = 1;
  int max_size = 10000000;

  int initial_stride = 1;
  int stride_step = 2;
  int max_stride = 32;
  int initial_block_stride = 1;
  int block_stride_step = 10;
  int block_max_stride = 1000000;
  int block_size = 1000;

  int num_repetitions = 10;

  printf("Block Stride A, Block Stride B, Stride A, StrideB, SizeA, SizeB, Time\n");
  for (int t = initial_block_stride; t <= block_max_stride; t*=block_stride_step) {
    for (int tt = t; tt <= block_max_stride; tt*=block_stride_step) {
      for (int k = initial_stride; k <= max_stride; k*=stride_step) {
        for (int kk = k; kk <= max_stride; kk*=stride_step) {
          for (int i = initial_size; i <= max_size; i*=step) {
            utils::ObjectsPtr objectsA(session->NewObjects());
            fill_objects(objectsA.get(), i, 0, k, block_size, t);
            for (int j = i; j <= max_size; j*=step) {
              utils::ObjectsPtr objectsB(session->NewObjects());
              fill_objects(objectsB.get(), j, 0, kk,block_size, tt);
              float total = 0;
              for(int q = 0; q < num_repetitions; ++q ) {
                clock_t start = clock();
                utils::ObjectsPtr(gdb::Objects::CombineIntersection(objectsA.get(), objectsB.get()));
                total += (clock() - start)/(float)CLOCKS_PER_SEC;
              }
              printf("%d, %d, %d, %d, %lld, %lld, %f\n", t, tt, k, kk, objectsA->Count(), objectsB->Count(), total / num_repetitions );
            }
          }
        }
      }
    }
  }
}
