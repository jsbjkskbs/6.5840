package shardctrler

import (
	"math"
	"sort"
)

func nextShard(index int) int {
	return (index + 1) % NShards
}

func (config *Config) safetyRemalloc() {
	// safety remalloc
	if config.Groups == nil {
		config.Groups = map[int][]string{}
	}

	if config.buffer == nil {
		config.buffer = map[int][]string{}
	}
}

// bufferRegister registers the groups to the buffer
func (config *Config) bufferRegister(id int, servers []string) {
	config.safetyRemalloc()

	if _, exist := config.Groups[id]; !exist && len(config.Groups) == NShards {
		config.buffer[id] = servers
	}
}

// bufferRemove removes the groups from the buffer
func (config *Config) bufferRemove(list []int) {
	config.safetyRemalloc()

	for _, id := range list {
		delete(config.buffer, id)
	}
}

// bufferApply applies the buffer to the configuration
func (config *Config) bufferApply() {
	if config.buffer == nil {
		config.buffer = map[int][]string{}
		return
	}

	list := []int{}
	for key := range config.buffer {
		list = append(list, key)
	}
	sort.Ints(list)

	newCount := NShards - len(config.Groups)
	for i := 0; i < newCount && i < len(list); i++ {
		key := list[i]
		config.Groups[key] = config.buffer[key]
		delete(config.buffer, key)
	}
}

// groupRemove removes the groups from the configuration
func (config *Config) groupRemove(list []int) {
	for _, id := range list {
		delete(config.Groups, id)
	}
	for index, id := range config.Shards {
		if _, exist := config.Groups[id]; !exist {
			config.Shards[index] = 0
		}
	}
}

// copy copies the configuration from another configuration
func (config *Config) copy(another *Config) {
	config.safetyRemalloc()
	another.safetyRemalloc()

	for i, key := range another.Groups {
		config.Groups[i] = key
	}

	for i, key := range another.buffer {
		config.buffer[i] = key
	}
}

// getState returns the current state of the shards
func (config *Config) getState() (map[int]int, int, int) {
	var (
		shard     = make(map[int]int)
		maxLength = 0
		maxCount  = 0
	)

	// count the number of shards each group has
	for _, gid := range config.Shards {
		shard[gid]++
		if shard[gid] > maxLength {
			maxLength = shard[gid]
		}
	}

	// count the number of groups that have the maximum number of shards
	for _, gid := range config.Shards {
		if shard[gid] == maxLength {
			maxCount++
		}
	}

	// return the current state of the shards
	return shard, maxLength, maxCount
}

// getMinGid returns the group with the minimum number of shards
func (config *Config) getMinGid() []int {
	var (
		gids      = []int{}
		shardCount = map[int]int{}
		minLength  = math.MaxInt
	)

	// count the number of shards each group has
	for _, id := range config.Shards {
		if _, exist := config.Groups[id]; exist {
			shardCount[id]++
		}
	}

	// count the minimum number of shards
	for id := range config.Groups {
		if _, exist := shardCount[id]; !exist {
			shardCount[id] = 0
		}
	}

	// get the minimum number of shards
	for _, count := range shardCount {
		if count < minLength {
			minLength = count
		}
	}

	// get the group with the minimum number of shards
	for id, count := range shardCount {
		if count == minLength {
			gids = append(gids, id)
		}
	}
	sort.Ints(gids)

	return gids
}

// NewConfig creates a new configuration
func NewConfig(me int, configs []Config, Groups map[int][]string) Config {
	current := configs[len(configs)-1]

	// create a new configuration
	new := makeConfig(current.Num+1, current.Shards)
	new.copy(&current)

	// register the new groups
	list := []int{}
	for gid := range Groups {
		list = append(list, gid)
	}
	sort.Ints(list)

	newGroup := []int{}
	for _, gid := range list {
		if len(new.Groups) < NShards {
			new.Groups[gid] = Groups[gid]
			newGroup = append(newGroup, gid)
		} else {
			new.bufferRegister(gid, Groups[gid])
		}
	}

	// apply the new groups
	if len(current.Groups) == 0 {
		// if there are no groups, assign the shards to the new groups
		for shard := 0; shard < NShards; shard++ {
			index := shard % len(newGroup)
			new.Shards[shard] = newGroup[index]
		}
	} else {
		// if there are groups, reassign the shards to the new groups
		shard, maxLength, maxCount := new.getState()
		for _, newGid := range newGroup {
			shard[newGid] = 0
			
			for index := 0; shard[newGid]+1 < maxLength; index = nextShard(index) {
				oldGid := new.Shards[index]
				if shard[oldGid] == maxLength {
					new.Shards[index] = newGid
					maxCount -= shard[oldGid]
					shard[oldGid]--
					shard[newGid]++

					if maxCount == 0 {
						shard, maxLength, maxCount = new.getState()
					}
				}
			}
		}
	}
	return new
}

func RemoveGroup(me int, configs []Config, list []int) Config {
	current := configs[len(configs)-1]
	// create a new configuration
	new := makeConfig(current.Num+1, current.Shards)
	new.copy(&current)
	new.bufferRemove(list)
	new.groupRemove(list)
	new.bufferApply()

	minGids := new.getMinGid()

	// reassign the shards to the new groups
	if len(new.Groups) > 0 {
		for shard, gid := range new.Shards {
			if gid != 0 {
				continue
			}
			new.Shards[shard] = minGids[0]
			minGids = minGids[1:]

			if len(minGids) == 0 {
				minGids = new.getMinGid()
			}
		}
	} else {
		new.Shards = [NShards]int{}
	}
	return new
}

// MoveShard moves a shard to a new group
func MoveShard(me int, configs []Config, nshard int, nid int) Config {
	current := configs[len(configs)-1]
	new := makeConfig(current.Num+1, [NShards]int{})
	new.copy(&current)

	for shard, id := range current.Shards {
		if shard != nshard {
			new.Shards[shard] = id
		} else {
			new.Shards[nshard] = nid
		}
	}
	return new
}

// QueryConfig queries the configuration
func QueryConfig(me int, configs []Config, num int) Config {
	if len(configs) == 0 {
		return Config{}
	}

	current := configs[len(configs)-1]
	if num == -1 || num >= current.Num {
		return current
	}

	return configs[num]
}

func makeConfig(num int, shards [NShards]int) Config {
	return Config{
		Num:    num,
		Shards: shards,
		Groups: map[int][]string{},
		buffer: map[int][]string{},
	}
}
