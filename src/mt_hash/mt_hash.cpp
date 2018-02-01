#include <python2.7/Python.h>


int64_t murmur_hash64_a(const void* key, size_t len, unsigned int seed) {
  const uint64_t m = 0xc6a4a7935bd1e995;
  const int r = 47;

  uint64_t h = seed ^ (len * m);

  const uint64_t* data = (const uint64_t*)key;
  const uint64_t* end = data + (len/8);

  while(data != end) {
    uint64_t k = *data++;
    k *= m;
    k ^= k >> r;
    k *= m;

    h ^= k;
    h *= m;
  }

  const char* tail = (const char*)data;
  switch(len & 7) {
    case 7:
      h ^= uint64_t(tail[6]) << 48;
    case 6:
      h ^= uint64_t(tail[5]) << 40;
    case 5:
      h ^= uint64_t(tail[4]) << 32;
    case 4:
      h ^= uint64_t(tail[3]) << 24;
    case 3:
      h ^= uint64_t(tail[2]) << 16;
    case 2:
       h ^= uint64_t(tail[1]) << 8;
    case 1:
      h ^= uint64_t(tail[0]);
      h *= m;
  }

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

static PyObject *hash(PyObject *self, PyObject *args) {
  int seed = 0;
  char* key = NULL;

  int ok = PyArg_ParseTuple(args, "si", &key, &seed);
  if (!ok) {
    return NULL;
  }

  size_t length = strlen(key);
  int64_t value = murmur_hash64_a(key, length, seed);
  return Py_BuildValue("l", value);
}

static PyMethodDef methods[] = {
    {"hash", hash, METH_VARARGS,
        "Get murmur hash for a string using the specified seed. jedis hash seed: <0x1234ABCD>\n"
        ":param key: string to be hashed.\n"
        ":param seed: hash seed.\n"
        ":returns: hash result.\n"
        ":type key: str\n"
        ":type seed: int\n"
        ":return: long"},
    {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC initmt_hash() {
  (void)Py_InitModule("mt_hash", methods);
}
