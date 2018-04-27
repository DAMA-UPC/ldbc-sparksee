

#include "gdb/Sparksee.h"
#include "gdb/Database.h"
#include "gdb/Session.h"
#include "gdb/Graph.h"
#include "gdb/Objects.h"
#include "gdb/ObjectsIterator.h"
#include "gdb/Exception.h"
#include "script/ScriptParser.h"
#include <iostream>

#include "queries/Utils.h"
#include "queries/TypeCache.h"
#include "utils/Utils.h"

#include "threads/Threads.h"

using namespace sparksee::gdb;

SparkseeConfig cfg;
Sparksee *gdb;
Database *db;
int main(int argc, char *argv[]) {

	std::string str(argv[1]);

  int num_threads = atoi(argv[2]);
	gdb = new Sparksee(cfg);
	db = gdb->Open(std::wstring(str.begin(), str.end()), false);

  
  timespec start, finish;
  clock_gettime(CLOCK_MONOTONIC, &start);

	std::cout << "Precomputing person similarities using " << num_threads << " threads" << std::endl;

  int block_size = 1000000;

  std::vector<std::thread> threads;
  for( int i = 0; i < num_threads; ++i ) {
          threads.push_back(std::thread( [&,i] () {
          std::cout << "Thread " << i << " spawned" << std::endl;
          Session* sess = db->NewSession();
        	Graph* graph = sess->GetGraph();

          sparksee::snb::TypeCache* cache = sparksee::snb::TypeCache::instance(graph); 
          attr_t weight_t = cache->knows_similarity_t;

          sess->Begin();
            Value val;
            std::vector<double> similarities;
            std::vector<oid_t>  edges;

            long long num_total = 0;
            long long num_block = 0;

            auto f = [&]{
                  std::cout << "Thread " << i << " is materializing similarities " << std::endl;
                  for(unsigned int j = 0; j < similarities.size(); ++j) {
                    double similarity = similarities[j];
                    oid_t edge = edges[j];
                    val.SetDouble(similarity);
                    graph->SetAttribute(edge,weight_t,val);
                  }
                  num_block = 0;
                  similarities.clear();
                  edges.clear();
            };

            Objects* knows = graph->Select(cache->knows_t);
            ObjectsIterator* iter_knows = knows->Iterator();

            while(iter_knows->HasNext()) {
                oid_t edge = iter_knows->Next();
                if((edge % (num_threads)) == i) {
                  EdgeData* eData = graph->GetEdgeData(edge);
                  double similarity = sparksee::snb::utils::similarity(*sess, *graph, *cache, eData->GetTail(), eData->GetHead());
                  similarities.push_back(similarity);
                  edges.push_back(edge);
                  num_block++;
                  num_total++;
                }

                if(num_block == block_size) {
                  f();
                  std::cout << "Thread " << i << " computed " << num_total << " similarities " << std::endl;
                }
            }

            if(num_block > 0 ) {
              f();
            }

            {
            int count = 0;
            std::cout << "Computing attribute for Posts" << std::endl;
            sparksee::utils::ObjectsPtr posts(graph->Select(cache->post_t));
            sparksee::utils::ObjectsIteratorPtr iter_posts(posts->Iterator());
            while(iter_posts->HasNext()) {
              oid_t post = iter_posts->Next();
              if((post % num_threads) == i ) {
              graph->GetAttribute(post,cache->post_creation_date_t, val);
              long long creation_date = val.GetTimestamp();
              sparksee::utils::ObjectsPtr countries(graph->Neighbors(post, cache->is_located_in_t, Outgoing ));
              graph->GetAttribute(countries->Any(),cache->place_id_t, val );
              long long country_id = val.GetLong();
              val.SetLong(sparksee::snb::utils::country_date(country_id, creation_date));
              graph->SetAttribute(post, cache->post_country_date_t, val);
              count++;
              if(count % 10000 == 0) std::wcout << "Thread " << i << "Computed attribute for " << count << " posts out of " << posts->Count() << std::endl;
              }
            }
            }

            {
            std::cout << "Computing attribute for Comments" << std::endl;
            int count = 0;
            sparksee::utils::ObjectsPtr comments(graph->Select(cache->comment_t));
            sparksee::utils::ObjectsIteratorPtr iter_comments(comments->Iterator());
            while(iter_comments->HasNext()) {
              oid_t comment = iter_comments->Next();
              if((comment % num_threads) == i ) {
              graph->GetAttribute(comment, cache->comment_id_t,val);
              graph->GetAttribute(comment,cache->comment_creation_date_t, val);
              long long creation_date = val.GetTimestamp();
              sparksee::utils::ObjectsPtr countries(graph->Neighbors(comment, cache->is_located_in_t, Outgoing ));
              graph->GetAttribute(countries->Any(),cache->place_id_t, val );
              long long country_id = val.GetLong();
              val.SetLong(sparksee::snb::utils::country_date(country_id, creation_date));
              graph->SetAttribute(comment, cache->comment_country_date_t, val);
              count++;
              if(count % 10000 == 0) std::cout << "Thread " << i << "Computed attribute for " << count << " comments out of " << comments->Count() << std::endl;
              }
            }
            }
            sess->Commit();
            delete iter_knows;
            delete knows;
            delete graph;
            delete sess;
       }));
       
  }


  set_thread_affinity(threads);

  for( std::thread &t : threads) {
    t.join();
  }

	delete db;
	db = NULL;
	delete gdb;
	gdb = NULL;

  clock_gettime(CLOCK_MONOTONIC, &finish);
  float elapsed = (finish.tv_sec - start.tv_sec);
  elapsed += (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
  std::cout << "Precompute time: " << elapsed << std::endl;
	return 0;
}
