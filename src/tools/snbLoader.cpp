#include "snbLoader.h"

#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <map>
#include <stdio.h>
#include <cstring>
#include <ctime>

#include <utils/Utils.h>

#include <assert.h>

#include "gdb/Sparksee.h"
#include "gdb/Database.h"
#include "gdb/Session.h"
#include "gdb/Graph.h"
#include "gdb/Objects.h"
#include "gdb/ObjectsIterator.h"
#include "gdb/Exception.h"
#include "io/CSVReader.h"
#include "io/NodeTypeLoader.h"
#include "io/EdgeTypeLoader.h"
#include <boost/date_time/local_time/local_time.hpp>

#include "queries/Utils.h"
#include "queries/TypeCache.h"

using namespace sparksee::gdb;
using namespace sparksee::io;
using namespace std;

#define NUM_LOADING_PARTITIONS 1

namespace snb_loader {

SparkseeConfig cfg;
Sparksee *gdb;
Database *db;
Session *sess;
Graph *graph;
int runs;

std::wstring dbname;
std::wstring datapath;
sparksee::gdb::int32_t partitions;
sparksee::gdb::int32_t thread_partitions;
bool_t profile;
std::map<std::wstring, std::wstring> m_params;

enum {
  CMD_OK,
  CMD_ERROR,
  CMD_SYNTAX
};

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

int GetBoolProp(std::vector<std::string> &tokens, bool_t &prop,
                const uchar_t *name) {
  if (tokens.size() != 3) {
    return CMD_SYNTAX;
  }

  if (tokens[2] == "true") {
    prop = true;
  } else if (tokens[2] == "false") {
    prop = false;
  } else {
    return CMD_SYNTAX;
  }

  std::wcout << L"SET " << name << L" '" << (prop ? L"TRUE" : L"FALSE") << L"'";
  return CMD_OK;
}

int RunSet(std::vector<std::string> &tokens, int line) {
  int error = CMD_OK;
  if (tokens.size() == 1) {
    error = CMD_SYNTAX;
  } else {
    if (tokens[1] == "db") {
      if (tokens.size() == 3) {
        dbname.assign(tokens[2].begin(), tokens[2].end());
        std::wcout << L"SET DB '" << dbname << L"'";
      } else {
        error = CMD_SYNTAX;
      }
    } else if (tokens[1] == "data") {
      if (tokens.size() == 3) {
        datapath.assign(tokens[2].begin(), tokens[2].end());
        std::wcout << L"SET DATA '" << datapath << L"'";
      } else {
        error = CMD_SYNTAX;
      }
    } else if (tokens[1] == "license") {
      if (tokens.size() == 3) {
        std::wstring s;
        s.assign(tokens[2].begin(), tokens[2].end());
        cfg.SetLicense(s);
        std::wcout << L"SET LICENSE '" << s << L"'";
      } else {
        error = CMD_SYNTAX;
      }
    } else if (tokens[1] == "param") {
      if (tokens.size() == 4) {
        std::wstring p, v;
        p.assign(tokens[2].begin(), tokens[2].end());
        v.assign(tokens[3].begin(), tokens[3].end());
        m_params[p] = v;
        std::wcout << L"SET PARAM '" << p << L"' = '" << v << L"'";
      } else {
        error = CMD_SYNTAX;
      }
    } else if (tokens[1] == "partitions") {
      if (tokens.size() == 3) {
        std::stringstream ss(tokens[2]);
        if ((ss >> partitions) && (partitions > 0)) {
          std::wcout << L"SET PARTITIONS " << partitions;
        } else {
          error = CMD_SYNTAX;
        }
      } else {
        error = CMD_SYNTAX;
      }
    } else if (tokens[1] == "thread_partitions") {
      if (tokens.size() == 3) {
        std::stringstream ss(tokens[2]);
        if ((ss >> thread_partitions) && (thread_partitions > 0)) {
          std::wcout << L"SET THREAD PARTITIONS " << thread_partitions;
        } else {
          error = CMD_SYNTAX;
        }
      } else {
        error = CMD_SYNTAX;
      }
    } else if (tokens[1] == "runs") {
      if (tokens.size() == 3) {
        std::stringstream ss(tokens[2]);
        if ((ss >> runs) && (runs > 0)) {
          std::wcout << L"SET RUNS " << runs;
        } else {
          error = CMD_SYNTAX;
        }
      } else {
        error = CMD_SYNTAX;
      }
    } else if (tokens[1] == "profile") {
      error = GetBoolProp(tokens, profile, L"PROFILE");
    } else {
      std::wcout << L"ERROR in line " << line << L": unknown set '"
                 << std::wstring(tokens[1].begin(), tokens[1].end()) << L"'"
                 << std::endl;
      std::wcout.flush();
      error = CMD_ERROR;
    }
  }
  return error;
}

int RunCreate(std::vector<std::string> &tokens, int line) {
  int error = CMD_OK;
  if (tokens.size() != 1) {
    error = CMD_SYNTAX;
  } else if (db != NULL) {
    std::wcout << L"ERROR in line " << line << L": db already open"
               << std::endl;
    std::wcout.flush();
    error = CMD_ERROR;
  } else {
    gdb = new Sparksee(cfg);
    db = gdb->Create(dbname.c_str(), L"ldbc");
    delete db;
    db = NULL;
    delete gdb;
    gdb = NULL;
    std::wcout << L"CREATED '" << dbname << L"'";
  }
  return error;
}

int RunOpen(std::vector<std::string> &tokens, int line) {
  int error = CMD_OK;
  if (tokens.size() != 1) {
    error = CMD_SYNTAX;
  } else if (db != NULL) {
    std::wcout << L"ERROR in line " << line << L": db already open"
               << std::endl;
    std::wcout.flush();
    error = CMD_ERROR;
  } else {
    gdb = new Sparksee(cfg);
    db = gdb->Open(dbname.c_str(), false);
    sess = db->NewSession();
    graph = sess->GetGraph();
    std::wcout << L"OPENED '" << dbname << L"'";
    sess->Begin();
  }
  return error;
}


void ComputeCountryDateAttribute(sparksee::snb::TypeCache* cache) {
	Value val;
	int count = 0;
	long long comment_id = -1;
	try{
		std::wcout << L"Computing attribute for Posts" << std::endl;
		sparksee::utils::ObjectsPtr posts(graph->Select(cache->post_t));
		sparksee::utils::ObjectsIteratorPtr iter_posts(posts->Iterator());
		while(iter_posts->HasNext()) {
			oid_t post = iter_posts->Next();
			graph->GetAttribute(post,cache->post_creation_date_t, val);
			long long creation_date = val.GetTimestamp();
			sparksee::utils::ObjectsPtr countries(graph->Neighbors(post, cache->is_located_in_t, Outgoing ));
			graph->GetAttribute(countries->Any(),cache->place_id_t, val );
			long long country_id = val.GetLong();
			val.SetLong(sparksee::snb::utils::country_date(country_id, creation_date));
			graph->SetAttribute(post, cache->post_country_date_t, val);
			count++;
			if(count % 10000 == 0) std::wcout << L"Computed attribute for " << count << " posts out of " << posts->Count() << std::endl;
		}

		std::wcout << L"Computing attribute for Comments" << std::endl;
		count = 0;
		sparksee::utils::ObjectsPtr comments(graph->Select(cache->comment_t));
		sparksee::utils::ObjectsIteratorPtr iter_comments(comments->Iterator());
		while(iter_comments->HasNext()) {
			oid_t comment = iter_comments->Next();
			graph->GetAttribute(comment, cache->comment_id_t,val);
			comment_id = val.GetLong();
			graph->GetAttribute(comment,cache->comment_creation_date_t, val);
			long long creation_date = val.GetTimestamp();
			sparksee::utils::ObjectsPtr countries(graph->Neighbors(comment, cache->is_located_in_t, Outgoing ));
			graph->GetAttribute(countries->Any(),cache->place_id_t, val );
			long long country_id = val.GetLong();
			val.SetLong(sparksee::snb::utils::country_date(country_id, creation_date));
			graph->SetAttribute(comment, cache->comment_country_date_t, val);
			count++;
			if(count % 10000 == 0) std::wcout << L"Computed attribute for " << count << L" comments out of " << comments->Count() << std::endl;
		}
	} catch ( Exception& e ) {
		std::cout << e.Message() << std::endl;
		std::wcout << L"Count: " << count << std::endl;
		std::wcout << L"Comment Id: " << comment_id << std::endl;
		exit(1);
	}
}

int RunClose(std::vector<std::string> &tokens, int line) {

  int error = CMD_OK;
  if (tokens.size() != 1) {
    error = CMD_SYNTAX;
  } else if (db == NULL) {
    std::wcout << L"ERROR in line " << line << L": db not open" << std::endl;
    std::wcout.flush();
    error = CMD_ERROR;
  } else {
    sess->Commit();
    delete graph;
    graph = NULL;
    delete sess;
    sess = NULL;
    delete db;
    db = NULL;
    delete gdb;
    gdb = NULL;
    std::wcout << L"CLOSED";
  }
  return error;
}

int RunDump(std::vector<std::string> &tokens, int line) {
  int error = CMD_OK;
  if (tokens.size() != 1) {
    error = CMD_SYNTAX;
  } else if (db == NULL) {
    std::wcout << L"ERROR in line " << line << L": db not open" << std::endl;
    std::wcout.flush();
    error = CMD_ERROR;
  } else {
    std::wstring s(dbname);
    s += L".dump";

    graph->DumpStorage(s.c_str());
    std::wcout << L"DUMPED TO '" << s << L"'";
  }
  return error;
}

int GetAttr(std::vector<std::string> &tokens, unsigned int index, type_t type,
            AttributeList &attrs, Int32List &columns) {
  int column = 0;
  while (index < tokens.size()) {
    if (!tokens[index].empty()) {
      std::vector<std::string> elems;
      std::stringstream ss(tokens[index]);
      std::string elem;
      while (std::getline(ss, elem, ':')) {
        elems.push_back(elem);
      }
      if ((elems.size() < 2) || (elems.size() > 3)) {
        return CMD_SYNTAX;
      }
      if (elems[0].empty()) {
        return CMD_SYNTAX;
      }
      if (elems[1].size() != 1) {
        return CMD_SYNTAX;
      }

      DataType dt;
      switch (elems[1].at(0)) {
      case 'S':
        dt = String;
        break;
      case 'I':
        dt = Integer;
        break;
      case 'L':
        dt = Long;
        break;
      case 'D':
        dt = Double;
        break;
      case 'B':
        dt = Boolean;
        break;
      case 'T':
        dt = Timestamp;
        break;
      case 'O':
        dt = OID;
        break;
      default:
        return CMD_SYNTAX;
      }
      AttributeKind kind = Basic;
      if (elems.size() == 3) {
        if (elems[2].size() != 1) {
          return CMD_SYNTAX;
        }
        switch (elems[2].at(0)) {
        case 'U':
          kind = Unique;
          break;
        case 'X':
          kind = Indexed;
          break;
        default:
          return CMD_SYNTAX;
        }
      }

      if (elems[0].at(0) != '?') {
        std::wstring name(elems[0].begin(), elems[0].end());
        attr_t attr = graph->FindAttribute(type, name.c_str());
        if (attr == Attribute::InvalidAttribute) {
          attr = graph->NewAttribute(type, name.c_str(), dt, kind);
        }
        attrs.Add(attr);
        columns.Add(column);
      }
    }
    index++;
    column++;
  }
  return CMD_OK;
}

int RunNodes(std::vector<std::string> &tokens, int line) {
  int error = CMD_OK;
  do {
    if (tokens.size() < 4) {
      error = CMD_SYNTAX;
      break;
    }

    if (db == NULL) {
      std::wcout << L"ERROR in line " << line << L": db not open" << std::endl;
      std::wcout.flush();
      error = CMD_ERROR;
      break;
    }

    std::wstring s;
    s.assign(tokens[2].begin(), tokens[2].end());
    type_t type = graph->FindType(s);
    if (type == Type::InvalidType) {
      type = graph->NewNodeType(s.c_str());
    }

    AttributeList attrs;
    Int32List columns;
    error = GetAttr(tokens, 3, type, attrs, columns);
    if (error != CMD_OK) {
      break;
    }

    for (int f = 0; f < partitions && error == CMD_OK; f++) {
       for( int p = 0; p < thread_partitions; p++) { 
           char filename[300];
           sprintf(filename, tokens[1].c_str(), f, p);

           std::string sz = filename;

           s = datapath;
           s.insert(s.end(), sz.begin(), sz.end());
           std::wcout << L"LOADING NODES IN '" << s << L"':";
           std::wcout.flush();

           CSVReader *reader = new CSVReader();
           reader->SetSeparator(L"|");
           reader->SetStartLine(1);
           reader->SetLocale(L"en_US.utf8");
           reader->Open(s.c_str());

           NodeTypeLoader *loader =
               new NodeTypeLoader(*reader, *graph, type, attrs, columns);
           //loader->SetTimestampFormat(L"yyyy-MM-dd hh:mm:ss.SSS");
           loader->SetTimestampFormat(L"yyyy-MM-ddThh:mm:ss.SSS+0000");
           loader->Run();

           Objects *objs = graph->Select(type);
           std::wcout << L"  " << objs->Count() << L" nodes" << std::endl;;
           delete objs;

           delete loader;
           delete reader;
      }
    }
  } while (false);

  return error;
}

class EdgesReader : public CSVReader {
public:
  EdgesReader(bool_t undirected, int tail, int head)
      : m_undirected(undirected), m_tail(tail), m_head(head) {}

  virtual sparksee::gdb::bool_t
  Read(sparksee::gdb::StringList &row) throw(sparksee::gdb::IOException) {
    bool_t rc = CSVReader::Read(row);
    while (m_undirected && rc) {
      sparksee::gdb::int64_t sc = 0, dc = 0;
      int i = 0;
      StringListIterator *it = row.Iterator();
      while (it->HasNext()) {
        const std::wstring &s = it->Next();
        if (i == m_tail) {
          std::wstringstream ss(s);
          ss >> sc;
        } else if (i == m_head) {
          std::wstringstream ss(s);
          ss >> dc;
        }
        i++;
      }
      delete it;

      if (sc < dc) {
        break;
      }

      rc = CSVReader::Read(row);
    }
    return rc;
  }

private:
  bool_t m_undirected;
  int m_tail;
  int m_head;
};

int RunEdges(std::vector<std::string> &tokens, int line) {
  int error = CMD_OK;
  do {
    if (tokens.size() < 11) {
      error = CMD_SYNTAX;
      break;
    }

    if (db == NULL) {
      std::wcout << L"ERROR in line " << line << L": db not open" << std::endl;
      std::wcout.flush();
      error = CMD_ERROR;
      break;
    }

    bool_t directed = true;
    bool_t materialized = false;
    const char *p = tokens[1].c_str();
    while (*p) {
      switch (*p) {
      case 'D':
        directed = true;
        break;
      case 'U':
        directed = false;
        break;
      case 'M':
        materialized = true;
        break;
      default:
        std::wcout << L"ERROR in line " << line << L": invalid edge modifier '"
                   << *p << L"'" << std::endl;
        std::wcout.flush();
        error = CMD_ERROR;
        break;
      }
      p++;
    }
    if (error != CMD_OK) {
      break;
    }

    std::wstring s;
    s.assign(tokens[5].begin(), tokens[5].end());
    type_t type = graph->FindType(s);
    if (type == Type::InvalidType) {
      std::wcout << L"ERROR: invalid type '"
                 << std::wstring(tokens[5].begin(), tokens[5].end()) << L"'"
                 << std::endl;
      std::wcout.flush();
      error = CMD_ERROR;
      break;
    }
    s.assign(tokens[6].begin(), tokens[6].end());
    attr_t a1 = graph->FindAttribute(type, s);
    if (a1 == Attribute::InvalidAttribute) {
      std::wcout << L"ERROR: invalid attribute '"
                 << std::wstring(tokens[5].begin(), tokens[5].end()) << L"."
                 << std::wstring(tokens[6].begin(), tokens[6].end()) << L"'"
                 << std::endl;
      std::wcout.flush();
      error = CMD_ERROR;
      break;
    }

    s.assign(tokens[8].begin(), tokens[8].end());
    type = graph->FindType(s);
    if (type == Type::InvalidType) {
      std::wcout << L"ERROR: invalid type '"
                 << std::wstring(tokens[8].begin(), tokens[8].end()) << L"'"
                 << std::endl;
      std::wcout.flush();
      error = CMD_ERROR;
      break;
    }
    s.assign(tokens[9].begin(), tokens[9].end());
    attr_t a2 = graph->FindAttribute(type, s);
    if (a2 == Attribute::InvalidAttribute) {
      std::wcout << L"ERROR: invalid attribute '"
                 << std::wstring(tokens[8].begin(), tokens[8].end()) << L"."
                 << std::wstring(tokens[9].begin(), tokens[9].end()) << L"'"
                 << std::endl;
      std::wcout.flush();
      error = CMD_ERROR;
      break;
    }

    unsigned int sc, dc;
    std::stringstream ss1(tokens[4]);
    if (!(ss1 >> sc) || (sc >= tokens.size() - 10)) {
      std::wcout << L"ERROR: invalid source column" << std::endl;
      std::wcout.flush();
      error = CMD_ERROR;
      break;
    }
    std::stringstream ss2(tokens[7]);
    if (!(ss2 >> dc) || (dc >= tokens.size() - 10)) {
      std::wcout << L"ERROR: invalid destination column" << std::endl;
      std::wcout.flush();
      error = CMD_ERROR;
      break;
    }

    s.assign(tokens[3].begin(), tokens[3].end());
    type = graph->FindType(s);
    if (type == Type::InvalidType) {
      type = graph->NewEdgeType(s.c_str(), directed, materialized);
    }

    AttributeList attrs;
    Int32List columns;
    error = GetAttr(tokens, 10, type, attrs, columns);
    if (error != CMD_OK) {
      break;
    }

    for (int f = 0; f < partitions && error == CMD_OK; f++) {
       for( int p = 0; p < thread_partitions; p++) { 
           char filename[300];
           sprintf(filename, tokens[2].c_str(), f, p);

           std::string sz = filename;

           s = datapath;
           s.insert(s.end(), sz.begin(), sz.end());
           std::wcout << L"LOADING EDGES IN '" << s << L"':";
           std::wcout.flush();

           CSVReader *reader = new EdgesReader(!directed, sc, dc);
           reader->SetSeparator(L"|");
           reader->SetStartLine(1);
           reader->SetLocale(L"en_US.utf8");
           reader->Open(s.c_str());

           EdgeTypeLoader *loader = new EdgeTypeLoader(*reader, *graph, type, attrs,
                   columns, dc, sc, a2, a1, IsError, IsError);
           //loader->SetTimestampFormat(L"yyyy-MM-dd hh:mm:ss.SSS");
           loader->SetTimestampFormat(L"yyyy-MM-ddThh:mm:ss.SSS+0000");
           loader->Run();

           Objects *objs = graph->Select(type);
           std::wcout << L"  " << objs->Count() << L" edges" << std::endl;
           delete objs;

           delete loader;
           delete reader;
      }
    }
  } while (false);

  return error;
}

int RunValues(std::vector<std::string> &tokens, int line) {
  int error = CMD_OK;
  do {
    std::wstring s;

    if (tokens.size() != 6) {
      error = CMD_SYNTAX;
      break;
    }

    if (db == NULL) {
      std::wcout << L"ERROR in line " << line << L": db not open" << std::endl;
      std::wcout.flush();
      error = CMD_ERROR;
      break;
    }

    // get the source column
    s.assign(tokens[2].begin(), tokens[2].end());
    type_t type = graph->FindType(s.c_str());
    if (type == Type::InvalidType) {
      std::wcout << L"ERROR: invalid type '"
                 << std::wstring(tokens[2].begin(), tokens[2].end()) << L"'"
                 << std::endl;
      std::wcout.flush();
      error = CMD_ERROR;
      break;
    }
    s.assign(tokens[3].begin(), tokens[3].end());
    attr_t asrc = graph->FindAttribute(type, s.c_str());
    if (asrc == Attribute::InvalidAttribute) {
      std::wcout << L"ERROR: invalid attribute '"
                 << std::wstring(tokens[2].begin(), tokens[2].end()) << L"."
                 << std::wstring(tokens[3].begin(), tokens[3].end()) << L"'"
                 << std::endl;
      std::wcout.flush();
      error = CMD_ERROR;
      break;
    }

    // get the edge type
    s.assign(tokens[4].begin(), tokens[4].end());
    type = graph->FindType(s.c_str());
    if (type == Type::InvalidType) {
      type = graph->NewEdgeType(s.c_str(), true, false);
    }

    // get the dictionary node type
    s.assign(tokens[5].begin(), tokens[5].end());
    type_t dtype = graph->FindType(s.c_str());
    if (dtype == Type::InvalidType) {
      dtype = graph->NewNodeType(s.c_str());
    }

    attr_t adst = graph->FindAttribute(dtype, s.c_str());
    if (adst == Attribute::InvalidAttribute) {
      adst = graph->NewAttribute(dtype, s.c_str(), String, Unique);
    }

    for (int f = 0; f < partitions && error == CMD_OK; f++) {
        for( int p = 0; p < thread_partitions; p++) { 
            char filename[300];
            sprintf(filename, tokens[1].c_str(), f, p);

            std::string sz = filename;

            s = datapath;
            s.insert(s.end(), sz.begin(), sz.end());
            std::wcout << L"LOADING VALUES IN '" << s << L"':";
            std::wcout.flush();

            CSVReader *reader = new CSVReader();
            reader->SetSeparator(L"|");
            reader->SetStartLine(1);
            reader->SetLocale(L"en_US.utf8");
            reader->Open(s.c_str());

            Value value;
            StringList row;
            while (reader->Read(row)) {
                StringListIterator *it = row.Iterator();
                if (!it->HasNext()) {
                    std::wcout << L"ERROR: missing key" << std::endl;
                    std::wcout.flush();
                    error = CMD_ERROR;
                    break;
                }

                sparksee::gdb::int64_t key;
                std::wstringstream ss(it->Next());
                if (!(ss >> key)) {
                    std::wcout << L"ERROR: invalid key" << std::endl;
                    std::wcout.flush();
                    error = CMD_ERROR;
                    break;
                }

                value.SetLong(key);
                oid_t osrc = graph->FindObject(asrc, value);
                if (osrc == Objects::InvalidOID) {
                    std::wcout << L"ERROR: unknown key " << key << std::endl;
                    std::wcout.flush();
                    error = CMD_ERROR;
                    break;
                }

                if (!it->HasNext()) {
                    std::wcout << L"ERROR: missing value" << std::endl;
                    std::wcout.flush();
                    error = CMD_ERROR;
                    break;
                }
                value.SetString(it->Next());
                oid_t odst = graph->FindObject(adst, value);
                if (odst == Objects::InvalidOID) {
                    odst = graph->NewNode(dtype);
                    graph->SetAttribute(odst, adst, value);
                }

                if (it->HasNext() && !it->Next().empty()) {
                    std::wcout << L"ERROR: too many values" << std::endl;
                    std::wcout.flush();
                    error = CMD_ERROR;
                    break;
                }

                graph->NewEdge(type, osrc, odst);

                delete it;
            }

            Objects *objs = graph->Select(type);
            std::wcout << L"  " << objs->Count() << L" edges" << std::endl;
            delete objs;

            delete reader;
        }
    }
  } while (false);

  return error;
}

int RunIndex(std::vector<std::string> &tokens, int line) {
    int error = CMD_OK;
    do {
        std::wstring s;

        if (tokens.size() != 3) {
            error = CMD_SYNTAX;
            break;
        }

        if (db == NULL) {
            std::wcout << L"ERROR in line " << line << L": db not open" << std::endl;
            std::wcout.flush();
            error = CMD_ERROR;
            break;
        }

        // get the column
        s.assign(tokens[1].begin(), tokens[1].end());
        type_t type = graph->FindType(s.c_str());
    if (type == Type::InvalidType) {
      std::wcout << L"ERROR: invalid type '"
                 << std::wstring(tokens[1].begin(), tokens[1].end()) << L"'"
                 << std::endl;
      std::wcout.flush();
      error = CMD_ERROR;
      break;
    }
    s.assign(tokens[2].begin(), tokens[2].end());
    attr_t attr = graph->FindAttribute(type, s.c_str());
    if (attr == Attribute::InvalidAttribute) {
      std::wcout << L"ERROR: invalid attribute '"
                 << std::wstring(tokens[1].begin(), tokens[1].end()) << L"."
                 << std::wstring(tokens[2].begin(), tokens[2].end()) << L"'"
                 << std::endl;
      std::wcout.flush();
      error = CMD_ERROR;
      break;
    }

    graph->IndexAttribute(attr, Indexed);

    std::wcout << L"INDEXED '"
               << std::wstring(tokens[1].begin(), tokens[1].end()) << L"."
               << std::wstring(tokens[2].begin(), tokens[2].end()) << L"'";
    std::wcout.flush();
  } while (false);

  return error;
}

bool_t GetParam(const uchar_t *name, std::wstring &param, int line) {
  std::map<std::wstring, std::wstring>::iterator it = m_params.find(name);
  if (it != m_params.end()) {
    param = it->second;
    return true;
  }
  std::wcout << L"ERROR in line " << line << L": missing parameter '" << name
             << L"'" << std::endl;
  std::wcout.flush();
  return false;
}

bool_t GetParam(const uchar_t *name, sparksee::gdb::int64_t &param, int line) {
  std::wstring s;
  if (GetParam(name, s, line)) {
    std::wstringstream ss(s);
    if (ss >> param) {
      return true;
    }
    std::wcout << L"ERROR in line " << line << L": parameter '" << name
               << L"' is not int64" << std::endl;
    std::wcout.flush();
  }
  return false;
}

bool_t GetParam(const uchar_t *name, sparksee::gdb::int32_t &param, int line) {
  std::wstring s;
  if (GetParam(name, s, line)) {
    std::wstringstream ss(s);
    if (ss >> param) {
      return true;
    }
    std::wcout << L"ERROR in line " << line << L": parameter '" << name
               << L"' is not int32" << std::endl;
    std::wcout.flush();
  }
  return false;
}

bool_t GetParamDate(const uchar_t *name, sparksee::gdb::int64_t &param,
                    int line) {
  std::wstring s;
  if (GetParam(name, s, line)) {
    std::wstring sx = L"&";
    sx += s;
    sx += L"&";
    int y, m, d;
    if (swscanf(sx.c_str(), L"&%d-%d-%d&", &y, &m, &d) == 3) {
      Value v;
      v.SetTimestamp(y, m, d, 0, 0, 0, 0);
      param = v.GetTimestamp();
      return true;
    }
    std::wcout << L"ERROR in line " << line << L": parameter '" << name
               << L"' is not date" << std::endl;
    std::wcout.flush();
  }
  return false;
}

void CorrectTimestamp(Graph *graph, std::wstring typeName,
                      std::wstring attributeName) {
  int type = graph->FindType(typeName);
  int attr = graph->FindAttribute(type, attributeName);
  Objects *obj = graph->Select(type);
  ObjectsIterator *it = obj->Iterator();
  Value v;
  std::string local_time_zone("");
  int i = 0;
  while (it->HasNext()) {
    i++;
    long long int oid = it->Next();
    graph->GetAttribute(oid, attr, v);
    long long original = v.GetTimestamp();
    long t = original / 1000;
    struct tm *gmttm = gmtime(&t);
    gmttm->tm_isdst = -1;
    long long int gmttime = mktime(gmttm);
    long long int diffTime = t - gmttime;
    v.SetTimestamp(original + diffTime * 1000);
    graph->SetAttribute(oid, attr, v);
  }
  std::wcout << L"NUMBER FIXED " << i << std::endl;
  std::wcout.flush();
  delete it;
  delete obj;
}

int Parse(const char *filename) {
  std::wcout << L"LDBC Test" << std::endl;
  std::wcout.flush();
  gdb = NULL;
  db = NULL;
  sess = NULL;
  graph = NULL;
  runs = 1;

  FILE *cmds = fopen(filename, "r");
  if (cmds == NULL) {
    std::wcout << L"ERROR: missing file '" << filename << L"'" << std::endl;
    std::wcout.flush();
    return 1;
  }

  int error = CMD_OK;
  int line = 0;
  char buff[2048];
  std::vector<std::string> tokens;
  PlatformStatistics st1, st2;

  try {
    time_t t1 = time(NULL);

    while ((error == CMD_OK) && !feof(cmds) &&
           (fgets(buff, 2047, cmds) != NULL)) {
      ++line;
      if (buff[0] == '%') {
        // comment
        continue;
      }

      int len = strlen(buff) - 1;
      if (buff[len] == '\n') {
        buff[len] = '\0';
      }

      std::stringstream ss(buff);
      std::string token;
      tokens.clear();
      while (std::getline(ss, token, ',')) {
        tokens.push_back(token);
      }

      if (tokens.empty()) {
        continue;
      }

      Platform::GetStatistics(st1);
      if (tokens[0] == "set") {
        error = RunSet(tokens, line);
      } else if (tokens[0] == "create") {
        error = RunCreate(tokens, line);
      } else if (tokens[0] == "open") {
        error = RunOpen(tokens, line);
      } else if (tokens[0] == "close") {
        error = RunClose(tokens, line);
      } else if (tokens[0] == "dump") {
        error = RunDump(tokens, line);
      } else if (tokens[0] == "nodes") {
        error = RunNodes(tokens, line);
      } else if (tokens[0] == "edges") {
        error = RunEdges(tokens, line);
      } else if (tokens[0] == "values") {
        error = RunValues(tokens, line);
      } else if (tokens[0] == "index") {
        error = RunIndex(tokens, line);
      } else if (tokens[0] == "correctTimestamps") {
        std::wcout << "Starting correcting timestamps" << std::endl;
        try {
          Sparksee *sparksee = new Sparksee(cfg);
          Database *db = sparksee->Open(dbname.c_str(), false);
          Session *sess = db->NewSession();
          Graph *graph = sess->GetGraph();
          CorrectTimestamp(graph, L"post", L"creationDate");
          CorrectTimestamp(graph, L"comment", L"creationDate");
          CorrectTimestamp(graph, L"person", L"creationDate");
          CorrectTimestamp(graph, L"forum", L"creationDate");
          CorrectTimestamp(graph, L"knows", L"creationDate");
          CorrectTimestamp(graph, L"hasMember", L"joinDate");
          CorrectTimestamp(graph, L"hasMemberWithPosts", L"joinDate");
          CorrectTimestamp(graph, L"likes", L"creationDate");
          delete graph;
          delete sess;
          delete db;
          delete sparksee;
        }
        catch (const Exception &e) {
          std::wcout << "Error detected. Do you have closed the database?"
                     << std::endl;
          std::cout << e.Message() << std::endl;
        }
      } else {
        std::wcout << L"ERROR in line " << line << L": unknown command '"
                   << std::wstring(tokens[0].begin(), tokens[0].end()) << L"'"
                   << std::endl;
        std::wcout.flush();
        error = CMD_ERROR;
      }

      if (error == CMD_OK) {
        Platform::GetStatistics(st2);
        std::wcout << L" in " << (st2.GetRealTime() - st1.GetRealTime()) /
                                     1000.0 << L" ms" << std::endl;
        std::wcout.flush();
      }
    }

    time_t t2 = time(NULL);
    int seconds = difftime(t2, t1);
    struct tm *timeinfo;
    timeinfo = localtime(&t1);
    std::string s = asctime(timeinfo);
    std::wcout << L"START TIME AT " << std::wstring(s.begin(), s.end());
    timeinfo = localtime(&t2);
    s = asctime(timeinfo);
    std::wcout << L"END TIME AT " << std::wstring(s.begin(), s.end());
    std::wcout << L"TIME " << (seconds / 3600) << L":"
               << ((seconds % 3600) / 60) << L":" << (seconds % 60)
               << std::endl;
  }
  catch (Exception &excp) {
    std::string s = excp.Message();
    std::wcout << L"ERROR in line " << line << L": exception '"
               << std::wstring(s.begin(), s.end()) << L"'" << std::endl;
    std::wcout.flush();
    error = CMD_ERROR;
  }
  fclose(cmds);

  if (error == CMD_SYNTAX) {
    std::wcout << L"ERROR in line " << line << L": syntax in '" << buff << L"'"
               << std::endl;
    std::wcout.flush();
  }

  if (graph != NULL) {
    delete graph;
  }
  if (sess != NULL) {
    delete sess;
  }
  if (db != NULL) {
    delete db;
  }
  if (gdb != NULL) {
    delete gdb;
  }

  std::wcout << L"bye." << std::endl;
  std::wcout.flush();
  return error;
}
}
