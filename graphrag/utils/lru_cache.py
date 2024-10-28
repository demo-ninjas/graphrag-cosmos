from typing import Any
from collections import OrderedDict

class _Item:
    prev:'_Item|None' = None
    next:'_Item|None' = None
    key:str
    value:Any

    def __init__(self, key:str, value:Any, prev:'_Item|None' = None, next:'_Item|None' = None) -> None:
        self.key = key
        self.value = value
        self.prev = prev
        self.next = next
        

class LRUCache:
    max_size:int
    cache:dict[str, _Item]
    head:_Item|None
    tail:_Item|None


    def __init__(self, max_size:int = 10_000):
        self.max_size = max_size
        self.cache = dict[str, _Item]()
        self.head = None
        self.tail = None
        

    def __getitem__(self, key):
        if key not in self.cache:
            return None
        
        item = self.cache[key]
        self._make_head(item)
        return item.value

    def __setitem__(self, key, value):
        if key in self.cache:
            item = self.cache[key]
            item.value = value
            self._make_head(item)
        else:
            item = _Item(key, value)
            self.cache[key] = item
            if self.head is None:
                self.head = item
                self.tail = item
            else:
                item.next = self.head
                self.head.prev = item
                self.head = item

            if len(self.cache) > self.max_size:
                if self.tail is not None:
                    del self.cache[self.tail.key]
                self.tail = self.tail.prev  # type: ignore
                self.tail.next = None       # type: ignore

    def __contains__(self, key):
        return key in self.cache

    def __len__(self):
        return len(self.cache)

    def __repr__(self):
        return repr(self.cache)

    def __str__(self):
        return str(self.cache)

    def __iter__(self):
        return iter(self.cache)

    def __reversed__(self):
        return reversed(self.cache)

    def __delitem__(self, key):
        item = self.cache.pop(key, None)
        if item is not None:
            if item == self.head:
                self.head = item.next
            if item == self.tail:
                self.tail = item.prev
            if item.prev is not None:
                item.prev.next = item.next
            if item.next is not None:
                item.next.prev = item.prev

    def _make_head(self, item):
        if item != self.head:
            if item == self.tail:
                self.tail = item.prev
                self.tail.next = None   # type: ignore
            else:
                item.prev.next = item.next  # type: ignore
                item.next.prev = item.prev  # type: ignore
            item.prev = None
            item.next = self.head
            self.head.prev = item # type: ignore
            self.head = item

    def clear(self):
        self.cache.clear()
        self.head = None
        self.tail = None

    def keys(self):
        return self.cache.keys()

    def values(self):
        return self.cache.values()

    def items(self):
        return self.cache.items()

    def pop(self, key):
        item = self.cache.pop(key, None)
        if item is not None:
            if item == self.head:
                self.head = item.next
            if item == self.tail:
                self.tail = item.prev
            if item.prev is not None:
                item.prev.next = item.next
            if item.next is not None:
                item.next.prev = item.prev
            return item.value
        return None

    def popitem(self, last=True):
        if self.tail is None: 
            return None
        if last:
            return self.pop(self.tail.key)
        else:
            return self.pop(self.head.key if self.head is not None else None)

    def get(self, key, default=None):
        val = self.__getitem__(key)
        return val if val is not None else default

    def __eq__(self, other):
        return self.cache == other

    def __ne__(self, other):
        return self.cache != other

    def __lt__(self, other):
        return self.cache < other

    def __le__(self, other):
        return self.cache <= other

    def __gt__(self, other):
        return self.cache > other

    def __ge__(self, other):
        return self.cache >= other

    def __hash__(self):
        return hash(self.cache)

    def __sizeof__(self):
        return self.cache.__sizeof__() + (2 * self.head.__sizeof__())*len(self.cache)

    def _debug_print(self, prefix:str|None = None): 
        print("Cache: " + ("" if prefix is None else " (" + prefix + ")"))
        print(" - LRU List:")
        node = self.head
        if node is None: 
            print("     - <empty>")
        else: 
            print(f" [{node.key}]", end="")
            node = node.next
        while node is not None:
            print(f" -> [{node.key}]", end="")
            node = node.next
        print("\n - Dict:")
        for k,v in self.cache.items():
            print(f"     - {k}: {v.value if v is not None else '<empty>'}")
        
        
    


## Main for testing
if __name__ ==  '__main__':
    cache = LRUCache(5)
    for i in range(1,6):
        cache[str(i)] = str(i)
    cache._debug_print("After 5")
    cache['6'] = '6'
    cache._debug_print("After 6")
    cache['7'] = '7'
    cache._debug_print("After 7")

    assert cache.get('5') == '5'
    cache._debug_print("Get 5")
    assert cache.get('5') == '5'
    cache._debug_print("Get 5 (2)")
    assert cache.get('6') == '6'
    cache._debug_print("Get 6")
    assert cache.get('2') is None
    cache._debug_print("Get 2 (should not exist)")
    assert cache.get('3') == '3'
    cache._debug_print("Get 3")