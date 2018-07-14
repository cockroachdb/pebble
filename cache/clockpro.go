// Package cache implements the CLOCK-Pro caching algorithm.
/*

CLOCK-Pro is a patent-free alternative to the Adaptive Replacement Cache,
https://en.wikipedia.org/wiki/Adaptive_replacement_cache .
It is an approximation of LIRS ( https://en.wikipedia.org/wiki/LIRS_caching_algorithm ),
much like the CLOCK page replacement algorithm is an approximation of LRU.

This implementation is based on the python code from https://bitbucket.org/SamiLehtinen/pyclockpro .

Slides describing the algorithm: http://fr.slideshare.net/huliang64/clockpro

The original paper: http://static.usenix.org/event/usenix05/tech/general/full_papers/jiang/jiang_html/html.html

It is MIT licensed, like the original.
*/
package cache

import "container/ring"

type pageType int

const (
	ptTest pageType = iota
	ptCold
	ptHot
)

func (p pageType) String() string {

	switch p {
	case ptTest:
		return "Test"
	case ptCold:
		return "Cold"
	case ptHot:
		return "Hot"
	}

	return "unknown"
}

type entry2 struct {
	ptype pageType
	key   string
	val   interface{}
	ref   bool
}

type Cache struct {
	mem_max  int
	mem_cold int
	keys     map[string]*ring.Ring

	hand_hot  *ring.Ring
	hand_cold *ring.Ring
	hand_test *ring.Ring

	count_hot  int
	count_cold int
	count_test int
}

func New(size int) *Cache {
	return &Cache{
		mem_max:  size,
		mem_cold: size,
		keys:     make(map[string]*ring.Ring),
	}
}

func (c *Cache) Get(key string) interface{} {

	r := c.keys[key]

	if r == nil {
		return nil
	}

	mentry := r.Value.(*entry2)

	if mentry.val == nil {
		return nil
	}

	mentry.ref = true
	return mentry.val
}

func (c *Cache) Set(key string, value interface{}) {

	r := c.keys[key]

	if r == nil {
		// no cache entry?  add it
		r = &ring.Ring{Value: &entry2{ref: false, val: value, ptype: ptCold, key: key}}
		c.meta_add(key, r)
		c.count_cold++
		return
	}

	mentry := r.Value.(*entry2)

	if mentry.val != nil {
		// cache entry was a hot or cold page
		mentry.val = value
		mentry.ref = true
		return
	}

	// cache entry was a test page
	if c.mem_cold < c.mem_max {
		c.mem_cold++
	}
	mentry.ref = false
	mentry.val = value
	mentry.ptype = ptHot
	c.count_test--
	c.meta_del(r)
	c.meta_add(key, r)
	c.count_hot++
}

func (c *Cache) meta_add(key string, r *ring.Ring) {

	c.evict()

	c.keys[key] = r
	r.Link(c.hand_hot)

	if c.hand_hot == nil {
		// first element
		c.hand_hot = r
		c.hand_cold = r
		c.hand_test = r
	}

	if c.hand_cold == c.hand_hot {
		c.hand_cold = c.hand_cold.Prev()
	}
}

func (c *Cache) meta_del(r *ring.Ring) {

	delete(c.keys, r.Value.(*entry2).key)

	if r == c.hand_hot {
		c.hand_hot = c.hand_hot.Prev()
	}

	if r == c.hand_cold {
		c.hand_cold = c.hand_cold.Prev()
	}

	if r == c.hand_test {
		c.hand_test = c.hand_test.Prev()
	}

	r.Prev().Unlink(1)
}

func (c *Cache) evict() {

	for c.mem_max <= c.count_hot+c.count_cold {
		c.run_hand_cold()
	}
}

func (c *Cache) run_hand_cold() {

	mentry := c.hand_cold.Value.(*entry2)

	if mentry.ptype == ptCold {

		if mentry.ref {
			mentry.ptype = ptHot
			mentry.ref = false
			c.count_cold--
			c.count_hot++
		} else {
			mentry.ptype = ptTest
			mentry.val = nil
			c.count_cold--
			c.count_test++
			for c.mem_max < c.count_test {
				c.run_hand_test()
			}
		}
	}

	c.hand_cold = c.hand_cold.Next()

	for c.mem_max-c.mem_cold < c.count_hot {
		c.run_hand_hot()
	}
}

func (c *Cache) run_hand_hot() {

	if c.hand_hot == c.hand_test {
		c.run_hand_test()
	}

	mentry := c.hand_hot.Value.(*entry2)

	if mentry.ptype == ptHot {

		if mentry.ref {
			mentry.ref = false
		} else {
			mentry.ptype = ptCold
			c.count_hot--
			c.count_cold++
		}
	}

	c.hand_hot = c.hand_hot.Next()
}

func (c *Cache) run_hand_test() {

	if c.hand_test == c.hand_cold {
		c.run_hand_cold()
	}

	mentry := c.hand_test.Value.(*entry2)

	if mentry.ptype == ptTest {

		prev := c.hand_test.Prev()
		c.meta_del(c.hand_test)
		c.hand_test = prev

		c.count_test--
		if c.mem_cold > 1 {
			c.mem_cold--
		}
	}

	c.hand_test = c.hand_test.Next()
}

func (c *Cache) dump() string {

	var b []byte

	var end *ring.Ring = nil
	for elt := c.hand_hot; elt != end; elt = elt.Next() {
		end = c.hand_hot
		m := elt.Value.(*entry2)

		if c.hand_hot == elt {
			b = append(b, '2')
		}

		if c.hand_test == elt {
			b = append(b, '0')
		}

		if c.hand_cold == elt {
			b = append(b, '1')
		}

		switch m.ptype {
		case ptHot:
			if m.ref {
				b = append(b, 'H')
			} else {
				b = append(b, 'h')
			}
		case ptCold:
			if m.ref {
				b = append(b, 'C')
			} else {
				b = append(b, 'c')
			}
		case ptTest:
			b = append(b, 'n')
		}
	}

	return string(b)
}
