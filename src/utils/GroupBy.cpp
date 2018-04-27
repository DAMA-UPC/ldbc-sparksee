
#include "GroupBy.h"
#include <gdb/Graph.h>
#include <gdb/Objects.h>
#include <gdb/ObjectsIterator.h>
#include <gdb/Session.h>
#include <gdb/Sparksee.h>
#include <gdb/Value.h>
#include <map>

namespace gdb = sparksee::gdb;

namespace sparksee {
        namespace utils {

            Group::Group( gdb::oid_t key, gdb::Objects* group) :
                key_(key),
                group_(group){

                }

            gdb::oid_t Group::key() const {
                return key_;
            }

            gdb::Objects* Group::group() const {
                return group_;
            }

            bool Group::operator<( const Group& g ) const {
                if( group_->Count() != g.group()->Count() ) {
                    return group_->Count() > g.group()->Count();
                }
                return key_< g.key();
            }

            GroupByIterator::GroupByIterator( GroupBy* group ):
                group_(group),
                it_(group->groups_.begin())

            {
            }

            GroupByIterator::~GroupByIterator() {
            }

            bool GroupByIterator::has_next() {
                return it_ != group_->groups_.end();
            }

            Group GroupByIterator::next() {
                Group g =  (*it_);
                ++it_;
                return g;
            }

            GroupBy::GroupBy(gdb::Session& sess , const gdb::Graph& graph) :
                sess_(sess),
                graph_(const_cast<gdb::Graph&>(graph)) {
                }

            GroupBy::~GroupBy() {
                for(std::set<Group>::iterator it = groups_.begin(); it != groups_.end(); ++it) {
                    Group group = *it;
                    delete group.group(); 
                }
            }

            void GroupBy::execute(gdb::Objects* edges, Mode mode ) {
                std::map<gdb::oid_t, gdb::Objects*> count;
                gdb::ObjectsIterator* iter_edges = edges->Iterator();
                while(iter_edges->HasNext()) {
                    gdb::oid_t edge = iter_edges->Next();
                    gdb::EdgeData* e_data = graph_.GetEdgeData(edge);
                    gdb::oid_t key;
                    gdb::oid_t other;
                    if( mode == kTail ) {
                        key = e_data->GetTail();
                        other = e_data->GetHead();
                    } else {
                        key = e_data->GetHead();
                        other = e_data->GetTail();
                    }
                    std::map<gdb::oid_t, gdb::Objects*>::iterator current = count.find(key);
                    if(current == count.end()) {
                        current = count.insert(std::pair<gdb::oid_t, gdb::Objects*>(key, sess_.NewObjects())).first;
                    }
                    current->second->Add(other);
                }
                delete iter_edges;
                for( std::map<gdb::oid_t, gdb::Objects*>::iterator it = count.begin(); it != count.end(); ++it){
                    Group group(it->first,it->second);
                    groups_.insert(group);
                }
            }

            GroupByIterator* GroupBy::iterator() {
                return new GroupByIterator(this);
            }

            long long GroupBy::count() const {
                return groups_.size();
            }
        }
}
