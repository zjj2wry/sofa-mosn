/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cluster

import (
	"math/rand"

	"github.com/alipay/sofa-mosn/pkg/api/v2"
	"github.com/alipay/sofa-mosn/pkg/log"
	"github.com/alipay/sofa-mosn/pkg/types"
)

type subSetLoadBalancer struct {
	lbType                types.LoadBalancerType // inner LB algorithm for choosing subset's host
	runtime               types.Loader
	stats                 types.ClusterStats
	random                rand.Rand
	fallBackPolicy        types.FallBackPolicy
	defaultSubSetMetadata types.SubsetMetadata        // default subset metadata
	subSetKeys            []types.SortedStringSetType // subset selectors
	originalPrioritySet   types.PrioritySet
	fallbackSubset        *LBSubsetEntry    // subset entry generated according to fallback policy
	subSets               types.LbSubsetMap // final trie-like structure used to stored easily searched subset
}

func NewSubsetLoadBalancer(lbType types.LoadBalancerType, prioritySet types.PrioritySet, stats types.ClusterStats,
	subsets types.LBSubsetInfo) types.SubSetLoadBalancer {

	ssb := &subSetLoadBalancer{
		lbType:                lbType,
		fallBackPolicy:        subsets.FallbackPolicy(),
		defaultSubSetMetadata: GenerateDftSubsetKeys(subsets.DefaultSubset()), //ordered subset metadata pair, value为md5 hash值
		subSetKeys:            subsets.SubsetKeys(),
		originalPrioritySet:   prioritySet,
		stats:                 stats,
	}

	ssb.subSets = make(map[string]types.ValueSubsetMap)  // 初始化字典树

	// foreach every priority subset
	// init subset, fallback subset and so on
	for _, hostSet := range prioritySet.HostSetsByPriority() {   // 创建 字典树 ，hostsRemoved 这里设置为空，在初始化的时候，不删除
		ssb.Update(hostSet.Priority(), hostSet.Hosts(), nil)
	}

	// add update callback when original priority set updated
	ssb.originalPrioritySet.AddMemberUpdateCb(
		func(priority uint32, hostsAdded []types.Host, hostsRemoved []types.Host) {
			ssb.Update(priority, hostsAdded, hostsRemoved)
		},
	)

	return ssb
}

// 根绝 metadata, 从字典树中匹配对应的 subset，然后在 subset 中根据常规的 LB 算法挑选 Host
func (sslb *subSetLoadBalancer) ChooseHost(context types.LoadBalancerContext) types.Host {

	if nil != context {
		host, hostChoosen := sslb.TryChooseHostFromContext(context)
		if hostChoosen {
			log.DefaultLogger.Debugf("subset load balancer: match subset entry success, "+
				"choose hostaddr = %s", host.AddressString())
			return host
		}
	}

	if nil == sslb.fallbackSubset {
		log.DefaultLogger.Errorf("subset load balancer: failure, fallback subset is nil")
		return nil
	}
	sslb.stats.LBSubSetsFallBack.Inc(1)

	defaulthosts := sslb.fallbackSubset.prioritySubset.GetOrCreateHostSubset(0).Hosts()

	if len(defaulthosts) > 0 {
		log.DefaultLogger.Debugf("subset load balancer: use default subset,hosts are ", defaulthosts)
	} else {
		log.DefaultLogger.Errorf("subset load balancer: failure, fallback subset's host is nil")
		return nil
	}

	return sslb.fallbackSubset.prioritySubset.LB().ChooseHost(context)
}

// GetHostsNumber used to get hosts' number for given matchCriterias
func (sslb *subSetLoadBalancer) GetHostsNumber(metadata types.MetadataMatchCriteria) uint32 {
	matchCriteria := metadata.MetadataMatchCriteria()

	if len(matchCriteria) == 0 {
		return 0
	}

	entry := sslb.FindSubset(matchCriteria)

	if entry == nil || !entry.Active() {
		return 0
	}

	return uint32(len(entry.PrioritySubset().GetOrCreateHostSubset(0).Hosts()))
}

// create or update subsets for this priority
func (sslb *subSetLoadBalancer) Update(priority uint32, hostAdded []types.Host, hostsRemoved []types.Host) {

	// step1. create or update fallback subset
	sslb.UpdateFallbackSubset(priority, hostAdded, hostsRemoved)

	// step2. create or update global subset
	// 更新 subset 的 callback
	
	// todo, 优化这里的方法
	sslb.ProcessSubsets(hostAdded, hostsRemoved,
		func(entry types.LBSubsetEntry) {
			activeBefore := entry.Active()
			entry.PrioritySubset().Update(priority, hostAdded, hostsRemoved)

			if activeBefore && !entry.Active() {
				sslb.stats.LBSubSetsActive.Dec(1)
				sslb.stats.LBSubsetsRemoved.Inc(1)
			} else if !activeBefore && entry.Active() {
				sslb.stats.LBSubSetsActive.Inc(1)
				sslb.stats.LBSubsetsCreated.Inc(1)
			}
		},
		
		// 创建 subset 中 LBSubsetEntry 的 hosts(prioritySubset) 的 callback
		// 这里也可以再优化
		func(entry types.LBSubsetEntry, predicate types.HostPredicate, kvs types.SubsetMetadata, addinghost bool) {
			if addinghost {
				prioritySubset := NewPrioritySubsetImpl(sslb, predicate)
				log.DefaultLogger.Debugf("creating subset loadbalancing for %+v", kvs)
				entry.SetPrioritySubset(prioritySubset)
				sslb.stats.LBSubSetsActive.Inc(1)
				sslb.stats.LBSubsetsCreated.Inc(1)
			}
			
		})
}

func (sslb *subSetLoadBalancer) TryChooseHostFromContext(context types.LoadBalancerContext) (types.Host, bool) {

	matchCriteria := context.MetadataMatchCriteria()

	if nil == matchCriteria {
		log.DefaultLogger.Errorf("subset load balancer: context is nil")
		return nil, false
	}

	entry := sslb.FindSubset(matchCriteria.MetadataMatchCriteria())   // 查询 subset

	if entry == nil || !entry.Active() || entry.PrioritySubset() == nil {   // 如果不存在这样的 subset
		log.DefaultLogger.Errorf("subset load balancer: match entry failure")
		return nil, false
	}

	return entry.PrioritySubset().LB().ChooseHost(context), true  // subset 存在的话，再根据常规 lb 算法那挑选
}

// create or update fallback subset
func (sslb *subSetLoadBalancer) UpdateFallbackSubset(priority uint32, hostAdded []types.Host,
	hostsRemoved []types.Host) {

	if types.NoFallBack == sslb.fallBackPolicy {
		log.DefaultLogger.Debugf("subset load balancer: fallback is disabled")
		return
	}

	// create default host subset
	if nil == sslb.fallbackSubset {
		var predicate types.HostPredicate

		if types.AnyEndPoint == sslb.fallBackPolicy {
			predicate = func(types.Host) bool {
				return true
			}
		} else if types.DefaultSubsetDefaultSubset == sslb.fallBackPolicy {
			predicate = func(host types.Host) bool {
				return sslb.HostMatches(sslb.defaultSubSetMetadata, host)
			}
		}

		sslb.fallbackSubset = &LBSubsetEntry{
			children:       nil, // children is nil for fallback subset
			prioritySubset: NewPrioritySubsetImpl(sslb, predicate),
		}
	}

	// update default host subset
	sslb.fallbackSubset.prioritySubset.Update(priority, hostAdded, hostsRemoved)
}

// create or update subset
func (sslb *subSetLoadBalancer) ProcessSubsets(hostAdded []types.Host, hostsRemoved []types.Host,
	updateCB func(types.LBSubsetEntry), newCB func(types.LBSubsetEntry, types.HostPredicate, types.SubsetMetadata, bool)) {
	
	// 依次遍历对所有添加的 host
		// 依次遍历所有 subset key, 看 host 的 metadata 是否有满足 subset key ，如果满足的话，
		// 需要根据 host 的值，创建一个子树，如果这棵子树已经存在的情况下，则添加到对应的 subset hosts 中去
	for _, host := range hostAdded {
		for _, subSetKey := range sslb.subSetKeys {
			
			kvs := sslb.ExtractSubsetMetadata(subSetKey.Keys(), host)   // 根据 host 携带的 metadata 创建查询子树
			if len(kvs) > 0 {
				
				// 查看 kvs 这棵子树是否已经存在，如果不存在的话，则创建，并返回叶子节点
				entry := sslb.FindOrCreateSubset(sslb.subSets, kvs, 0)
				
				if entry.Initialized() {   // 如果 叶子节点的 prioritySubset 已经有 host 了，调用回调函数
					updateCB(entry)
				} else {    // 否则的话，定义 predicate 函数，回调创建函数
					predicate := func(host types.Host) bool {
						return sslb.HostMatches(kvs, host)
					}

					newCB(entry, predicate, kvs, true)
				}
			}
		}
	}
	
	// 根据删除的 hosts，来更新字典树，和创建类似
	for _, host := range hostsRemoved {
		for _, subSetKey := range sslb.subSetKeys {
			
			kvs := sslb.ExtractSubsetMetadata(subSetKey.Keys(), host)
			if len(kvs) > 0 {
				
				entry := sslb.FindOrCreateSubset(sslb.subSets, kvs, 0)
				
				if entry.Initialized() {
					updateCB(entry)
				} else {
					predicate := func(host types.Host) bool {
						return sslb.HostMatches(kvs, host)
					}
					
					newCB(entry, predicate, kvs, false)
				}
			}
		}
	}
	
}

// judge whether the host's metatada match the subset
// kvs and host must in the same order
func (sslb *subSetLoadBalancer) HostMatches(kvs types.SubsetMetadata, host types.Host) bool {
	hostMetadata := host.Metadata()

	for _, kv := range kvs {
		if value, ok := hostMetadata[kv.T1]; ok {
			if !types.EqualHashValue(value, kv.T2) {
				return false
			}
		} else {
			return false
		}
	}

	return true
}

// search subset tree
func (sslb *subSetLoadBalancer) FindSubset(matchCriteria []types.MetadataMatchCriterion) types.LBSubsetEntry {
	// subsets : map[string]ValueSubsetMap
	// valueSubsetMap: map[HashedValue]LBSubsetEntry
	var subSets = sslb.subSets

	// 遍历所有输入的待 match 的 metadata ，依次从头结点遍历下去，如果全部匹配的话，则返回 entry.
	
	for i, mcCriterion := range matchCriteria {

		if vsMap, ok := subSets[mcCriterion.MetadataKeyName()]; ok {

			if vsEntry, ok := vsMap[mcCriterion.MetadataValue()]; ok {

				if i+1 == len(matchCriteria) {
					
					if vsEntry.PrioritySubset() != nil {
						return vsEntry
					} else {
						return nil;  // 如果 metadata 没有完全匹配的话，那么会导致 priority hosts 不存在
					}
					
				}

				subSets = vsEntry.Children()

			} else {
				break
			}
		} else {
			break
		}
	}

	return nil
}


// FindOrCreateSubset 为 subsets 中，根据 kvs (types.SubsetMetadata  创建查询子树的接口，如果查询子树已经存在，则返回叶子节点；
// idx 为当前树节点所在层次，在创建 subset 的时候，使用递归的方法创建，如果 idx 达到叶子节点，则返回，否则继续创建子节点

func (sslb *subSetLoadBalancer) FindOrCreateSubset(subsets types.LbSubsetMap,
	kvs types.SubsetMetadata, idx uint32) types.LBSubsetEntry {

	if idx > uint32(len(kvs)) {
		log.DefaultLogger.Fatal("Find Or Create Subset Error")
	}

	name := kvs[idx].T1  // 当前层的 subset key
	hashedValue := kvs[idx].T2  // 当前 key 对应的 value
	var entry types.LBSubsetEntry

	if vsMap, ok := subsets[name]; ok {   //  判断当前的 LbSubsetMap  是否已经存在 key

		if lbEntry, ok := vsMap[hashedValue]; ok {   // 如果 key 存在的话，查看 entry 是否存在
			// entry already exist
			entry = lbEntry
		} else {
			// 在 entry 不存在的情况下，创建 entry 但是注意这里并不创建 prioritySubset
			entry = &LBSubsetEntry{
				children:       make(map[string]types.ValueSubsetMap),
				prioritySubset: nil,
			}
			vsMap[hashedValue] = entry
		}
	} else { // 如果 key 不存在，那么要新建一个 ValueSubsetMap , entry 同时也要新建，并把新建的 map 加到 subsets 中
		entry = &LBSubsetEntry{
			children:       make(map[string]types.ValueSubsetMap),
			prioritySubset: nil,
		}

		valueSubsetMap := types.ValueSubsetMap{
			hashedValue: entry,
		}

		subsets[name] = valueSubsetMap
	}

	idx++

	if idx == uint32(len(kvs)) {    // 创建完毕返回叶子节点，叶子节点的 prioritySubset 要把当前的 host 加进去
		return entry
	}

	return sslb.FindOrCreateSubset(entry.Children(), kvs, idx)   // 递归的创建子节点。
}

// ExtractSubsetMetadata 用于将 host 携带的 meatadata 和 subset selector 中的值做匹配，如果 key 匹配
// 中了，那么会根据 key 对应 value 的值，按照字典序创建一个查询子树出来
func (sslb *subSetLoadBalancer) ExtractSubsetMetadata(subsetKeys []string, host types.Host) types.SubsetMetadata {
	metadata := host.Metadata()
	var kvs types.SubsetMetadata

	for _, key := range subsetKeys {
		exist := false
		var value types.HashedValue

		for keyM, valueM := range metadata {
			if keyM == key {
				value = valueM
				exist = true
				break
			}
		}

		if !exist {
			break
		} else {
			v := types.Pair{
				T1: key,
				T2: value,
			}
			kvs = append(kvs, v)
		}
	}

	if len(kvs) != len(subsetKeys) {   // key 完全匹配上了，返回这个 { key,value } 组合
		kvs = []types.Pair{}
	}

	return kvs
}

type LBSubsetEntry struct {
	children       types.LbSubsetMap
	prioritySubset types.PrioritySubset
}

func (lbbe *LBSubsetEntry) Initialized() bool {
	return nil != lbbe.prioritySubset
}

func (lbbe *LBSubsetEntry) Active() bool {
	return true
}

func (lbbe *LBSubsetEntry) PrioritySubset() types.PrioritySubset {
	return lbbe.prioritySubset
}

func (lbbe *LBSubsetEntry) SetPrioritySubset(ps types.PrioritySubset) {
	lbbe.prioritySubset = ps
}

func (lbbe *LBSubsetEntry) Children() types.LbSubsetMap {
	return lbbe.children
}

type hostSubsetImpl struct {
	hostSubset types.HostSet
}

// 根据新添加的查询子树，遍历所有的 hosts，看是否存在满足的，如果存在的话，则加入 subset 对应的 Hosts 集合中
func (hsi *hostSubsetImpl) UpdateHostSubset(hostsAdded []types.Host, hostsRemoved []types.Host, predicate types.HostPredicate) {
	// todo check host predicate

	var filteredAdded []types.Host   // 待添加的函数中，满足 predict 函数的 Hosts 的集合，predict 即查看，host 的 metadata 是否匹配
	var filteredRemoved []types.Host

	//var hosts []types.Host
	var healthyHosts []types.Host

	for _, host := range hostsAdded {
		if predicate(host) {
			filteredAdded = append(filteredAdded, host)
		}
	}

	for _, host := range hostsRemoved {
		if predicate(host) {
			filteredRemoved = append(filteredRemoved, host)
		}
	}

	finalhosts := hsi.GetFinalHosts(filteredAdded, filteredRemoved)   // 做一次去重
	
	// 获取健康的 机器列表
	for _, host := range finalhosts {

		if host.Health() {
			healthyHosts = append(healthyHosts, host)
		}
	}

	//最终更新 host
	hsi.hostSubset.UpdateHosts(finalhosts, healthyHosts, nil, nil,
		filteredAdded, filteredRemoved)
}


// 这里的去重，目前还是需要的，因为在创建数的时候，每出现一课新的子树的时候，是全量去查看其他树是否有满足的，所以会导致每个 host ，被查询多次
// todo 只更新当前 host 以前的
func (hsi *hostSubsetImpl) GetFinalHosts(hostsAdded []types.Host, hostsRemoved []types.Host) []types.Host {
	hosts := hsi.hostSubset.Hosts()

	for _, host := range hostsAdded {
		found := false
		for _, hostOrig := range hosts {
			if host.AddressString() == hostOrig.AddressString() {
				found = true
			}
		}

		if !found {
			hosts = append(hosts, host)
		}
	}

	for _, host := range hostsRemoved {
		for i, hostOrig := range hosts {
			if host.AddressString() == hostOrig.AddressString() {
				hosts = append(hosts[:i], hosts[i+1:]...)
				continue
			}
		}
	}

	return hosts
}

func (hsi *hostSubsetImpl) Empty() bool {
	return len(hsi.hostSubset.Hosts()) == 0
}

func (hsi *hostSubsetImpl) Hosts() []types.Host {
	return hsi.hostSubset.Hosts()
}

// subset of original priority set
type PrioritySubsetImpl struct {
	prioritySubset      types.PrioritySet // storing the matched host in subset
	originalPrioritySet types.PrioritySet
	empty               bool
	loadbalancer        types.LoadBalancer
	predicateFunc       types.HostPredicate // match function for host and metadata
}


// 创建 subset 对应的 hosts
func NewPrioritySubsetImpl(subsetLB *subSetLoadBalancer, predicate types.HostPredicate) types.PrioritySubset {
	psi := &PrioritySubsetImpl{
		originalPrioritySet: subsetLB.originalPrioritySet,
		predicateFunc:       predicate,
		empty:               true,
		prioritySubset:      &prioritySet{},
	}

	var i uint32

	for i = 0; i < uint32(len(psi.originalPrioritySet.HostSetsByPriority())); i++ {
		if len(psi.originalPrioritySet.GetOrCreateHostSet(i).Hosts()) > 0 {
			psi.empty = false
		}
	}

	// 遍历 original hosts ，如果 hosts 满足 predict 函数，即满足 Metadata 的匹配，则添加 hosts
	for i = 0; i < uint32(len(psi.originalPrioritySet.HostSetsByPriority())); i++ {
		psi.Update(i, subsetLB.originalPrioritySet.HostSetsByPriority()[i].Hosts(), []types.Host{})
	}

	psi.loadbalancer = NewLoadBalancer(subsetLB.lbType, psi.prioritySubset)

	return psi
}

func (psi *PrioritySubsetImpl) Update(priority uint32, hostsAdded []types.Host, hostsRemoved []types.Host) {

	psi.GetOrCreateHostSubset(priority).UpdateHostSubset(hostsAdded, hostsRemoved, psi.predicateFunc)  // predicate 函数是关键

	for _, hostSet := range psi.prioritySubset.HostSetsByPriority() {
		if len(hostSet.Hosts()) > 0 {
			psi.empty = false
			return
		}
	}
}

func (psi *PrioritySubsetImpl) Empty() bool {

	return psi.empty
}

func (psi *PrioritySubsetImpl) GetOrCreateHostSubset(priority uint32) types.HostSubset {
	return &hostSubsetImpl{
		hostSubset: psi.prioritySubset.GetOrCreateHostSet(priority),
	}
}

func (psi *PrioritySubsetImpl) TriggerCallbacks() {

}

func (psi *PrioritySubsetImpl) CreateHostSet(priority uint32) types.HostSet {
	return nil
}

func (psi *PrioritySubsetImpl) LB() types.LoadBalancer {
	return psi.loadbalancer
}

func NewLBSubsetInfo(subsetCfg *v2.LBSubsetConfig) types.LBSubsetInfo {
	lbSubsetInfo := &LBSubsetInfoImpl{
		fallbackPolicy: types.FallBackPolicy(subsetCfg.FallBackPolicy),
		subSetKeys:     GenerateSubsetKeys(subsetCfg.SubsetSelectors),
	}

	if len(subsetCfg.SubsetSelectors) == 0 {
		lbSubsetInfo.enabled = false
	} else {
		lbSubsetInfo.enabled = true
	}

	lbSubsetInfo.defaultSubSet = types.InitSortedMap(subsetCfg.DefaultSubset)

	return lbSubsetInfo
}

type LBSubsetInfoImpl struct {
	enabled        bool
	fallbackPolicy types.FallBackPolicy
	defaultSubSet  types.SortedMap             // sorted default subset
	subSetKeys     []types.SortedStringSetType // sorted subset selectors
}

func (lbsi *LBSubsetInfoImpl) IsEnabled() bool {
	return lbsi.enabled
}

func (lbsi *LBSubsetInfoImpl) FallbackPolicy() types.FallBackPolicy {
	return lbsi.fallbackPolicy
}

func (lbsi *LBSubsetInfoImpl) DefaultSubset() types.SortedMap {
	return lbsi.defaultSubSet
}

func (lbsi *LBSubsetInfoImpl) SubsetKeys() []types.SortedStringSetType {
	return lbsi.subSetKeys
}

// used to generate sorted keys
// without duplication
func GenerateSubsetKeys(keysArray [][]string) []types.SortedStringSetType {
	var ssst []types.SortedStringSetType

	for _, keys := range keysArray {
		sortedStringSet := types.InitSet(keys)
		found := false

		for _, s := range ssst {
			if judgeStringArrayEqual(sortedStringSet.Keys(), s.Keys()) {
				found = true
				break
			}
		}

		if !found {
			ssst = append(ssst, sortedStringSet)
		}
	}

	return ssst
}

func GenerateDftSubsetKeys(dftkeys types.SortedMap) types.SubsetMetadata {
	var sm types.SubsetMetadata
	for _, pair := range dftkeys {
		sm = append(sm, types.Pair{
			T1: pair.Key,
			T2: types.GenerateHashedValue(pair.Value),
		})
	}

	return sm
}

func judgeStringArrayEqual(T1 []string, T2 []string) bool {

	if len(T1) != len(T2) {
		return false
	}

	for i := 0; i < len(T1); i++ {
		if T1[i] != T2[i] {
			return false
		}
	}

	return true
}
