# Schemaless 在 CalmCore 中的实现与优化

## 概述

在 CalmCore 中，"schemaless" 指的是一种灵活的数据写入方式，其中系统不要求预先定义数据模式（schema）。这种方法允许根据输入数据的类型自动推导出schema的结构，极大地方便了用户的上手和使用。然而，这种方式可能无法很好地优化索引结构，尤其是在处理标量字符串时，系统缺乏判断其是应被视为关键词还是需要进行分词的能力。

## 早期实现

在早期版本中，CalmCore 通过使用record结构来处理数据插入。用户需要在record中设置field字段，以指定插入数据的索引类型及其值。这种方法虽然功能完备，但使得record结构较为复杂，对用户而言并不友好。

随后，尝试将record data转变为JSON类型，以期简化数据结构，但这一改变反而使得schemaless方式难以支持。

## 新功能介绍

为了克服上述问题并优化schemaless的实现，CalmCore 计划引入以下三个功能点：

1. Schemaless 字段
在schema结构体中增加一个名为schemaless的布尔类型字段。
若该字段设置为true，则表示JSON内的全部数据都将被索引。系统将根据字段类型自动构造出索引方式。例如，字符串字段默认被视为关键词（keyword）索引。
2. 动态增加索引接口：addIndex(Field)
允许用户通过addIndex(Field)接口动态地为特定space增加索引字段。
新增的索引仅对当前space有效，不会影响到全局schema。
3. 动态删除索引接口：delIndex(Field)
允许用户通过delIndex(Field)接口动态地为特定space删除索引字段。
删除的索引仅对当前space有效，同样不会影响到全局schema。
结论
通过引入上述功能，CalmCore 旨在提供更加灵活和高效的schemaless数据处理方式。这不仅简化了用户的操作流程，而且提高了系统的灵活性和可用性。尽管如此，用户仍需谨慎考虑使用schemaless方式可能带来的索引优化问题。
