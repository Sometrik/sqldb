#ifndef _UTILS_H_
#define _UTILS_H_

static inline std::string normalize_nfc(std::string_view input) {
  auto r0 = utf8proc_NFC(reinterpret_cast<const unsigned char *>(input.data()));
  std::string r;
  if (r0) {
    r = (const char *)r0;
    free(r0);
  }
  return r;
}

#endif
