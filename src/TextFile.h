#ifndef _TEXTFILE_H_
#define _TEXTFILE_H_

#include <string>
#include <cstdio>

namespace sqldb {
  class TextFile {
  public:
    enum class Error { OK = 0, Eof, InvalidUtf8 };
  
    TextFile() { }
    TextFile(std::string filename) : filename_(std::move(filename)) { }
    TextFile(const TextFile & other) : filename_(other.filename_) { }
    TextFile(TextFile && other) : filename_(std::move(other.filename_)) { }

    TextFile & operator=(const TextFile & other) {
      if (this != &other) {
	if (in_) {
	  fclose(in_);
	  in_ = nullptr;
	}
	filename_ = other.filename_;
      }
      return *this;
    }

    TextFile & operator=(TextFile && other) {
      if (this != &other) {
	std::swap(in_, other.in_);
	std::swap(filename_, other.filename_);
	std::swap(input_buffer_, other.input_buffer_);
      }
      return *this;
    }
    
    virtual ~TextFile() {
      if (in_) {
	fclose(in_);
      }
    }
    
  protected:
    std::string filename_;

    FILE * in_ = nullptr;
    std::string input_buffer_;  
  };
}

#endif
