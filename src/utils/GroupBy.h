
#ifndef GROUPBY_H_
#define GROUPBY_H_

#include <gdb/common.h>
#include <set>

namespace sparksee {

namespace gdb {
class Objects;
class Graph;
class Session;
class Value;
enum Condition;
enum EdgesDirection;
}


namespace utils {


    class Group {
        public:
            gdb::oid_t key() const ;
            gdb::Objects* group() const ;
            bool operator<( const Group& g ) const;
        private:
            friend class GroupByIterator;
            friend class GroupBy;
            Group( gdb::oid_t key, gdb::Objects* group);
            gdb::oid_t      key_;
            gdb::Objects*   group_;
    };

    class GroupBy;
    class GroupByIterator{
        public:
            ~GroupByIterator();
            bool has_next();
            Group next();

        private:
            friend class GroupBy;
            GroupByIterator( GroupBy* group );
            GroupBy*                   group_;
            std::set<Group>::iterator it_;
    };

    class GroupBy {
        public:
            enum Mode {
                kTail,
                kHead,
            };

            ~GroupBy();

            void execute(gdb::Objects* edges, Mode mode );
            GroupByIterator* iterator();
            long long count() const ; 
        private:

            friend class GroupByIterator;
            friend GroupBy* group_by( gdb::Session& sess, const gdb::Graph& graph, gdb::Objects* edges, GroupBy::Mode mode);

            GroupBy( gdb::Session& sess, const gdb::Graph& graph);
            std::set<Group> groups_;
            gdb::Session& sess_;
            gdb::Graph& graph_;
    };
}
}

#endif
