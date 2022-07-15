package kvraft

type MemoryKV struct {
	KV map[string]string
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{make(map[string]string)}
}

func (memoryKV *MemoryKV) Found(key string) bool {
	_, ok := memoryKV.KV[key]
	return ok
}

func (memoryKV *MemoryKV) Get(key string) (string, Err) {
	value, ok := memoryKV.KV[key]
	if ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (memoryKV *MemoryKV) Put(key, value string) Err {
	memoryKV.KV[key] = value
	return OK
}

func (memoryKV *MemoryKV) Append(key, value string) Err {
	memoryKV.KV[key] += value
	return OK
}
