package kvraft

type MemoryKV struct {
	KV map[string]string
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{
		KV: make(map[string]string),
	}
}
func (memoryKV *MemoryKV) GetKV() map[string]string {
	return memoryKV.KV
}
func (memoryKV *MemoryKV) SetKV(newKV map[string]string) {
	memoryKV.KV = newKV
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
