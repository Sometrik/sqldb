#ifndef _UTILS_H_
#define _UTILS_H_

#include <utf8proc.h>

static inline std::pair<std::string, int> normalize_nfc(std::string_view input) {
  if (input.empty()) {
    return std::pair(std::string(), 0);
  } else {
    utf8proc_uint8_t * dest = nullptr;
    // option UTF8PROC_STRIPCC is not used since it would remove tabs from tsv files
    auto options = utf8proc_option_t(UTF8PROC_IGNORE | UTF8PROC_COMPOSE | UTF8PROC_NLF2LF | UTF8PROC_LUMP );
    auto s = utf8proc_map(reinterpret_cast<const unsigned char *>(input.data()),
			  static_cast<utf8proc_ssize_t>(input.size()),
			  &dest,
			  options
			  );
    if (s >= 0) {
      std::string r(reinterpret_cast<char *>(dest), s);
      free(dest);
      return std::pair(std::move(r), 0);
    } else {
      free(dest);
      return std::pair(std::string(), static_cast<int>(s));
    }
  }
}

#endif
