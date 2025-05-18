package shardkv

type Shard struct {
	Store         map[string]string         // key-value store
	Status        ShardStatus               // status of the shard
	ClientContext map[string]OperationState // client context for each client
}

func NewShard() *Shard {
	return &Shard{
		Store:         make(map[string]string),
		Status:        ShardInvalid,
		ClientContext: make(map[string]OperationState),
	}
}

func (shard *Shard) Get(key string) (string, Err) {
	value, exists := shard.Store[key]
	if !exists {
		return "", ErrNoKey
	}
	return value, OK
}

func (shard *Shard) Put(key string, value string) Err {
	shard.Store[key] = value
	return OK
}

func (shard *Shard) Append(key string, value string) Err {
	shard.Store[key] += value
	return OK
}

func (shard *Shard) deepCopy() *Shard {
	newShard := &Shard{
		Store:         make(map[string]string),
		Status:        shard.Status,
		ClientContext: make(map[string]OperationState),
	}
	for key, value := range shard.Store {
		newShard.Store[key] = value
	}
	for clientID, state := range shard.ClientContext {
		newShard.ClientContext[clientID] = *state.deepCopy()
	}
	return newShard
}
