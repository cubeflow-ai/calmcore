# rust raft

### raft库比较

| 库名             | 简介                                                                                      | 主要特点                                                                                  | GitHub地址   | star                                                   |
|-----------------|------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------|------------------------------------------------------------|------------------------------------------------------------|
| **raft-rs (TiKV)** | 为分布式系统设计的具有容错能力的共识协议实现，是TiKV项目的一部分。                         | 适用于生产环境的稳健实现，被TiKV使用。                                                   | [https://github.com/tikv/raft-rs](https://github.com/tikv/raft-rs) | 2800 |
| **MadRaft**       | 基于MadSim的Raft共识算法实验室，部分代码源自MIT 6.824和PingCAP Talent Plan的Raft Lab。    | 适合学习目的，不建议用于生产。                                           | [https://github.com/madsim-rs/MadRaft](https://github.com/madsim-rs/MadRaft) | 104 |
| **riteraft**      | 在tikv/raft-rs之上创建的一个更易于使用和实现的层。                                       | 提供简化接口，便于快速入门和实现。                                                       | [https://github.com/PsiACE/riteraft](https://github.com/PsiACE/riteraft) | 309 |
| **toydb的Raft模块** | 用于线性一致状态机复制的基于Raft的分布式共识引擎，是toydb项目的一部分。                    | 专门针对toydb项目的Raft模块，可以参考。                                                                 | [https://github.com/erikgrinaker/toydb/tree/master/src/raft](https://github.com/erikgrinaker/toydb/tree/master/src/raft) | 5900 |
| **little-raft**  | 最轻量的分布式共识库。运行您自己的复制状态机！                                           | 轻量级实现，特色是代码保证在1000行内，方便二次开发。                                                         | [https://github.com/andreev-io/little-raft](https://github.com/andreev-io/little-raft) | 406 |
| **async-raft**   | 快速的Rust、现代共识协议以及可靠的异步运行时 —— 旨在为下一代分布式数据存储系统提供共识支持。| 支持异步运行，基本已经不维护了，几年前问过作者，作者建议用tikv的。                                             | [https://github.com/async-raft/async-raft](https://github.com/async-raft/async-raft) | 998 |
| **openraft**     | 旨在改进raft，作为下一代分布式数据存储系统的共识协议。                                    | 面向下一代分布式系统的改进型共识协议。在async-raft 基础上做的，但是几个数据库在使用，可能是生产可用状态                                                   | [https://github.com/datafuselabs/openraft](https://github.com/datafuselabs/openraft) | 1200 |

ps：目前来看有两个选择：
    *老牌的raft-rs，优点稳定可靠生产可用，缺点接口不友好，代码侵入高。
    * openraft，有几个star较高的db背书，meilisearch 尝试使用raft是openraft <https://github.com/meilisearch/minimeili-raft>

# 不用raft 换一种方式来搞

需要有一个选举策略，

1. 保证只有leader可写wal。
2. 其它flower 订阅wal的内容收到后进行日志应用

![image](https://github.com/ansjsun/calmcore_indicat/assets/1221947/33c805f1-4c8c-4838-be26-7d59ded3c96d)
