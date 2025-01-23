# MIT 6.5840(2024)

## 介绍
这是MIT 6.5840(2024)课程实验的代码仓库，涵盖了Raft算法的各个部分实现，包括Leader选举、日志复制、持久化和日志压缩等。

[MIT 6.5840(2024)](https://pdos.csail.mit.edu/6.824/)

目前已基本通过测试

## 学习笔记

你可以访问[飞书文档](https://r10u1xujw3v.feishu.cn/wiki/TihgwBwWjiV05hkhZroc2YtXnCg?from=from_copylink)或者[这个链接](https://www.sophisms.cn/document/12)

## 目录结构
```
.
│  .gitignore
│  Makefile
│  README.md
│
└─src
    │  .gitignore
    │  go.mod
    │  go.sum
    │
    ├─kvraft
    │      client.go
    │      common.go
    │      config.go
    │      server.go
    │      test_test.go
    │
    ├─kvsrv
    │      client.go
    │      common.go
    │      config.go
    │      server.go
    │      test_test.go
    │
    ├─labgob
    │      labgob.go
    │      test_test.go
    │
    ├─labrpc
    │      labrpc.go
    │      test_test.go
    │
    ├─main
    │      diskvd.go
    │      lockc.go
    │      lockd.go
    │      mrcoordinator.go
    │      mrsequential.go
    │      mrworker.go
    │      pbc.go
    │      pbd.go
    │      pg-being_ernest.txt
    │      pg-dorian_gray.txt
    │      pg-frankenstein.txt
    │      pg-grimm.txt
    │      pg-huckleberry_finn.txt
    │      pg-metamorphosis.txt
    │      pg-sherlock_holmes.txt
    │      pg-tom_sawyer.txt
    │      test-mr-many.sh
    │      test-mr.sh
    │      viewd.go
    │
    ├─models
    │      kv.go
    │
    ├─mr
    │      coordinator.go
    │      rpc.go
    │      worker.go
    │
    ├─mrapps
    │      crash.go
    │      early_exit.go
    │      indexer.go
    │      jobcount.go
    │      mtiming.go
    │      nocrash.go
    │      rtiming.go
    │      wc.go
    │
    ├─porcupine
    │      bitset.go
    │      checker.go
    │      model.go
    │      porcupine.go
    │      visualization.go
    │
    ├─raft
    │      config.go
    │      persister.go
    │      pkg.go
    │      raft.go
    │      test_test.go
    │      util.go
    │
    ├─shardctrler
    │      client.go
    │      common.go
    │      config.go
    │      pkg.go
    │      server.go
    │      test_test.go
    │
    └─shardkv
            client.go
            common.go
            config.go
            server.go
            server_config.go
            shard.go
            test_test.go
```