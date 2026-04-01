Redis lists are ordered sequences of strings, implemented as a doubly-linked list (or listpack for small lists). You can push/pop from both ends, making them great for queues, stacks, and activity feeds.

## Adding elements

```sh
LPUSH mylist a b c        # push to left (head) → ["c", "b", "a"]
RPUSH mylist x y z        # push to right (tail) → ["c", "b", "a", "x", "y", "z"]

LPUSHX mylist val         # only pushes if key already exists
RPUSHX mylist val         # same, right side

LINSERT mylist BEFORE "x" "new"   # insert "new" before "x"
LINSERT mylist AFTER  "x" "new"   # insert "new" after "x"
LSET mylist 2 "replaced"          # overwrite element at index 2
```

## Removing elements

```sh
LPOP mylist               # remove & return from left
RPOP mylist               # remove & return from right
LPOP mylist 3             # remove & return 3 elements from left
RPOP mylist 3             # remove & return 3 elements from right

LREM mylist 2  "a"        # remove 2 occurrences of "a" from head→tail
LREM mylist -2 "a"        # remove 2 occurrences from tail→head
LREM mylist 0  "a"        # remove ALL occurrences of "a"

LTRIM mylist 1 3          # keep only indices 1–3, delete everything else
```

## Reading elements
```
LRANGE mylist 0 -1        # get all elements (0 = first, -1 = last)
LRANGE mylist 0 4         # get first 5 elements
LRANGE mylist -3 -1       # get last 3 elements

LINDEX mylist 0           # get element at index 0 (head)
LINDEX mylist -1          # get element at last index (tail)

LLEN mylist               # number of elements in the list
```

## Blocking commands
```
# Blocks until an element is available (or timeout expires)
BLPOP mylist 10           # blocking LPOP, waits up to 10 seconds
BRPOP mylist 10           # blocking RPOP

# Block on multiple lists — returns from whichever gets an element first
BLPOP queue:high queue:low 30

# Blocking move
BLMOVE src dest LEFT RIGHT 10
```

## Moving between lists
```
LMOVE src dest LEFT  RIGHT   # pop from src left, push to dest right
LMOVE src dest RIGHT LEFT    # pop from src right, push to dest left
```## Real-world pattern examples

**Job queue** — producer pushes, consumer blocks waiting:
```
# Producer
RPUSH jobs:email '{"to":"alice@example.com","subject":"Welcome"}'

# Consumer (blocks until a job arrives, no busy-polling)
BLPOP jobs:email 0     # 0 = wait forever
```

**Stack** — undo history:
```
LPUSH undo:user:42 "delete_row_55"
LPUSH undo:user:42 "update_row_12"

LPOP undo:user:42      # → "update_row_12" (most recent first)
```

**Capped activity feed** — keep only the last 100 events:
```
LPUSH feed:user:42 '{"event":"liked","post":99}'
LTRIM feed:user:42 0 99       # trim to 100 items after every push

LRANGE feed:user:42 0 19      # read first page (20 items)
```

**Safe queue handoff with `LMOVE`** — move a job to a "processing" list atomically so it isn't lost if the worker crashes:
```
LMOVE jobs:pending jobs:processing LEFT LEFT

# after job is done:
LREM jobs:processing 1 "<job_payload>"
```

---

| Command | Direction | Blocking? |
|---|---|---|
| `LPUSH` / `RPUSH` | left / right | no |
| `LPOP` / `RPOP` | left / right | no |
| `BLPOP` / `BRPOP` | left / right | **yes** |
| `LMOVE` | configurable | no |
| `BLMOVE` | configurable | **yes** |
| `LRANGE` | read slice | no |
| `LINDEX` | read one | no |
| `LLEN` | count | no |
| `LREM` | remove by value | no |
| `LTRIM` | keep slice | no |
| `LINSERT` | insert by value | no |
| `LSET` | set by index | no |