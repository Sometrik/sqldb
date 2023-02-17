#ifndef _SQLDB_AUDIO_H_
#define _SQLDB_AUDIO_H_

#include "Table.h"
#include "Cursor.h"

#include <stdexcept>

namespace sqldb {
  class AudioFile;
  
  class Audio : public Table {
  public:
    Audio(std::string filename);
    Audio(const Audio & other);
    Audio(Audio && other);

    std::unique_ptr<Table> copy() const override { return std::make_unique<Audio>(*this); }
       
    int getNumFields() const override;    
    const std::string & getColumnName(int column_index) const override;
    ColumnType getColumnType(int column_index) const override;
    
    void clear() override {
      throw std::runtime_error("Audio is read-only");
    }

    void addColumn(std::string_view name, sqldb::ColumnType type, bool unique = false) override {
      throw std::runtime_error("Audio is read-only");
    }

    std::unique_ptr<Cursor> insert(const Key & key) override {
      throw std::runtime_error("Audio is read-only");
    }

    std::unique_ptr<Cursor> insert() override {
      throw std::runtime_error("Audio is read-only");
    }

    std::unique_ptr<Cursor> increment(const Key & key) override {
      throw std::runtime_error("Audio is read-only");
    }

    std::unique_ptr<Cursor> assign(std::vector<int> columns) override {
      throw std::runtime_error("Audio is read-only");
    }
    
    void remove(const Key & key) override {
      throw std::runtime_error("Audio is read-only");
    }

    std::unique_ptr<Cursor> seekBegin() override;
    std::unique_ptr<Cursor> seek(const Key & key) override;
    std::unique_ptr<Cursor> seek(long long from, long long to);
    
  private:
    // audio_ is shared with cursors
    std::shared_ptr<AudioFile> audio_;
  };
};

#endif
