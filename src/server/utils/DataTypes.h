

#ifndef DATATYPES_H_
#define DATATYPES_H_

#include <stdlib.h>

namespace datatypes{
  enum BufferType {
    E_RAW,
    E_STRING
  };
  struct Buffer{
    const char* buffer_;
    int         buffer_length_;
    BufferType  buffer_type_;

    Buffer( char* buffer, int length, BufferType type = E_STRING ) : buffer_(buffer), buffer_length_(length), buffer_type_(type) {}

    void free() {
      if(buffer_ != NULL) {
        delete [] buffer_;
        buffer_length_ = 0;
      }
    }
    
  };
}

#endif
