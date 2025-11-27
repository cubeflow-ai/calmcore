# 这个模块用来实现calm的分布式能力
    1. 我们的集群采用gossip协议，种子结点担任协调任务保持最新的状态。种子结点之间组成一个raft group。
    2. 其它结点分别管理 某个个table 的某个partiiton 。 
