package vfs

// memFSClone contains state used to clone a MemFS.
// It exists as a struct as opposed to just passing the map
// through some clone methods largely to facilitate cloning
// of files for testing.
type memFSClone struct {
	clonedFS    *MemFS
	clonedNodes map[*memNode]*memNode
}

func makeMemFSClone(src *MemFS) memFSClone {
	src.mu.Lock()
	defer src.mu.Unlock()
	cc := memFSClone{
		clonedNodes: make(map[*memNode]*memNode),
		clonedFS: &MemFS{
			strict:      src.strict,
			ignoreSyncs: src.ignoreSyncs,
		},
	}

	cc.clonedFS.root = cc.cloneNode(src.root)
	return cc
}

func (cc memFSClone) cloneNode(f *memNode) *memNode {
	if cloned, exists := cc.clonedNodes[f]; exists {
		return cloned
	}
	cloned := &memNode{
		name:           f.name,
		isDir:          f.isDir,
		refs:           f.refs,
		children:       make(map[string]*memNode, len(f.children)),
		syncedChildren: make(map[string]*memNode, len(f.syncedChildren)),
	}
	copyNodeData(cloned, f)
	cc.clonedNodes[f] = cloned
	for name, c := range f.children {
		cloned.children[name] = cc.cloneNode(c)
	}
	for name, c := range f.syncedChildren {
		cloned.syncedChildren[name] = cc.cloneNode(c)
	}
	return cloned
}

func copyNodeData(dst, src *memNode) {
	src.mu.Lock()
	defer src.mu.Unlock()
	dst.mu.data = src.mu.data
	dst.mu.modTime = src.mu.modTime
	dst.mu.syncedData = src.mu.syncedData
}
