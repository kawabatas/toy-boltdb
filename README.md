# toy-boltdb
(学習用)自作BoltDB - An embedded key/value database for Go

[Bolt](https://github.com/boltdb/bolt) is a pure Go key/value store.
This is a repository that I learn how databases work.

>- Bolt uses a B+tree internally so there can be a lot of random page access.
>- Bolt uses copy-on-write so old pages cannot be reclaimed while an old transaction is using them.
>- Once the transaction has been committed or rolled back then the memory they point to can be reused by a new page or can be unmapped from virtual memory.
>- Bolt uses an exclusive write lock on the database file so it cannot be shared by multiple processes.

# References
- [boltdb/bolt](https://github.com/boltdb/bolt)
