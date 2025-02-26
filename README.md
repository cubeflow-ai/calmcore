# CalmCore

<div align="center">
  <img src="assets/logo.png" alt="CalmCore Logo" width="300px">
  
  <h3>高性能搜索引擎核心库</h3>

  [English](README_en.md) | 简体中文

  <a href="https://github.com/your-username/calmcore/blob/master/LICENSE">
    <img src="https://img.shields.io/badge/license-Apache--2.0-blue" alt="license">
  </a>
  <a href="https://github.com/your-username/calmcore/releases">
    <img src="https://img.shields.io/github/v/release/your-username/calmcore" alt="release">
  </a>
  <a href="https://docs.rs/calmcore/">
    <img src="https://img.shields.io/badge/docs-latest-brightgreen.svg" alt="Documentation">
  </a>
  <a href="https://github.com/your-username/calmcore/actions">
    <img src="https://github.com/your-username/calmcore/workflows/CI/badge.svg" alt="build status">
  </a>
</div>

## 简介

CalmCore 是一个用 Rust 编写的高性能、轻量级搜索引擎核心库，针对云原生环境做了特殊的优化， 可以更加灵活的和分布式文件系统共同工作。它结合了多种检索技术，提供了高效的数据存储、索引和查询能力。

### 主要特点

- **高性能**：利用 Rust 的零成本抽象和内存安全特性，提供优异的查询性能
- **多种索引类型**：支持关键词、全文、数值、向量等多种索引类型
- **分段存储**：采用分段式存储架构，支持后台合并提高性能
- **丰富的数据类型**：
  - 整型 (int64)
  - 浮点型 (float64)
  - 字符串 (string)
  - 文本 (text)
  - 二进制数据 (bytes)
  - 向量 (vector)
- **丰富的查询能力**：
  - 精确查询
  - 范围查询 
  - 前缀查询
  - 全文搜索
  - 向量相似度查询
  - 多字段组合查询
- **多平台支持**：可在 Linux、macOS、Windows 等多种平台上运行

## 快速开始

### 安装

将 CalmCore 添加到你的 `Cargo.toml` 文件中：


## RoadMap
 *
