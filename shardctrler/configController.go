package shardctrler

type ConfigStateMachine interface {
	Join(servers map[int][]string) Err
	Leave(gids []int) Err
	Move(shard int, gid int) Err
	Query(num int) (Err, Config)
}

type ConfigMemStore struct {
	Configs []Config // indexed by config num
}

func initialConfig() Config {
	return Config{
		Num:    0,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}
}

func (cms *ConfigMemStore) currentConfig() Config {
	return cms.Configs[len(cms.Configs)-1]
}

func (cms *ConfigMemStore) Join(servers map[int][]string) Err {
	currentConfig := cms.currentConfig()

	newConfig := Config{
		Num:    currentConfig.Num + 1,
		Shards: currentConfig.Shards,
		Groups: deepCopy(currentConfig.Groups),
	}

	for gid, servers := range servers {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}

	shardMap := newConfig.GetGidToShardMap()
	for gid := range servers {
		if _, ok := shardMap[gid]; !ok {
			shardMap[gid] = make([]int, 0)
		}
	}

	for {
		source, target := getBalancingSource(shardMap), getBalancingTarget(shardMap)
		if source != 0 && len(shardMap[source])-len(shardMap[target]) <= 1 {
			break
		}

		shardMap[target] = append(shardMap[target], shardMap[source][0])
		shardMap[source] = shardMap[source][1:]
	}

	var newShards [NShards]int
	for gid, shards := range shardMap {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}

	newConfig.Shards = newShards
	cms.Configs = append(cms.Configs, newConfig)

	return OK
}

func (cms *ConfigMemStore) Leave(gids []int) Err {
	currentConfig := cms.currentConfig()

	newConfig := Config{
		Num:    currentConfig.Num + 1,
		Shards: currentConfig.Shards,
		Groups: deepCopy(currentConfig.Groups),
	}

	shardMap := newConfig.GetGidToShardMap()
	orphanedShards := make([]int, 0)
	for _, gid := range gids {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}

		if shards, ok := shardMap[gid]; ok {
			delete(shardMap, gid)
			for _, shard := range shards {
				orphanedShards = append(orphanedShards, shard)
			}
		}
	}

	var newShards [NShards]int
	if len(newConfig.Groups) > 0 {
		for _, shard := range orphanedShards {
			target := getBalancingTarget(shardMap)
			shardMap[target] = append(shardMap[target], shard)
		}

		for gid, shards := range shardMap {
			for _, shard := range shards {
				newShards[shard] = gid
			}

		}
	}
	newConfig.Shards = newShards
	cms.Configs = append(cms.Configs, newConfig)

	return OK
}

func (cms *ConfigMemStore) Move(shard int, gid int) Err {
	currentConfig := cms.currentConfig()

	newConfig := Config{
		Num:    currentConfig.Num + 1,
		Shards: currentConfig.Shards,
		Groups: deepCopy(currentConfig.Groups),
	}

	var newShards [NShards]int
	copy(newShards[:], newConfig.Shards[:])
	newShards[shard] = gid

	newConfig.Shards = newShards
	cms.Configs = append(cms.Configs, newConfig)

	return OK
}

func (cms *ConfigMemStore) Query(num int) (Err, Config) {
	if num < 0 || num >= len(cms.Configs) {
		return OK, cms.currentConfig()
	}
	return OK, cms.Configs[num]
}
