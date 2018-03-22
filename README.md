# MINIDKVS

MINIDKVS is a very simple distributed key-value store. It is meant to support
the use case where you have some small-ish database that needs to be
synchronized between multiple nodes. It is not distributed in the sense that
the data can be sharded across servers. A more realistic use case would be a
user settings database that should be synchronized across devices relatively
quickly.