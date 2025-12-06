# Requirements Document

## Introduction

本功能为 CalmCore Data Explorer Web 界面添加创建表和删除表的 DDL 功能，并增强底部控制台以支持 SQL 和 GraphQL 两种查询语言。用户可以通过 Web 界面直接管理数据库表结构，无需使用命令行工具。

## Glossary

- **DDL (Data Definition Language)**: 数据定义语言，用于创建、修改和删除数据库对象（如表）
- **GraphQL**: 一种 API 查询语言，CalmCore 使用它作为主要的 API 接口
- **SQL**: 结构化查询语言，用于数据查询和操作
- **Console**: Web 界面底部的查询控制台区域
- **Modal**: 弹出式对话框，用于表单输入

## Requirements

### Requirement 1

**User Story:** As a database administrator, I want to create new tables through the web interface, so that I can quickly set up data structures without using command-line tools.

#### Acceptance Criteria

1. WHEN a user clicks the "创建表" button in the table list header THEN the System SHALL display a modal dialog with table creation form
2. WHEN a user submits a valid table creation form THEN the System SHALL send a GraphQL createTable mutation to the server and create the table
3. WHEN table creation succeeds THEN the System SHALL close the modal, refresh the table list, and display a success notification
4. IF table creation fails THEN the System SHALL display an error message with the failure reason and keep the modal open
5. WHEN the modal is displayed THEN the System SHALL provide form fields for table name, description, primary key, partition strategy, and field definitions

### Requirement 2

**User Story:** As a database administrator, I want to delete existing tables through the web interface, so that I can remove unused tables safely.

#### Acceptance Criteria

1. WHEN a user clicks the delete button on a table item THEN the System SHALL display a confirmation dialog warning about data loss
2. WHEN a user confirms table deletion THEN the System SHALL send a GraphQL dropTable mutation to the server
3. WHEN table deletion succeeds THEN the System SHALL close the confirmation dialog, refresh the table list, and display a success notification
4. IF table deletion fails THEN the System SHALL display an error message with the failure reason
5. IF the deleted table was currently selected THEN the System SHALL clear the data preview area

### Requirement 3

**User Story:** As a developer, I want to execute both SQL and GraphQL queries from the console, so that I can use my preferred query language.

#### Acceptance Criteria

1. WHEN the console is displayed THEN the System SHALL show a language selector with "SQL" and "GraphQL" options
2. WHEN "SQL" mode is selected THEN the System SHALL wrap the SQL query in a GraphQL query mutation before sending
3. WHEN "GraphQL" mode is selected THEN the System SHALL send the query directly to the GraphQL endpoint
4. WHEN a user presses Ctrl+Enter THEN the System SHALL execute the current query in the selected language mode
5. WHEN query execution completes THEN the System SHALL display results in the data preview area

### Requirement 4

**User Story:** As a user, I want visual feedback during operations, so that I know the system is processing my requests.

#### Acceptance Criteria

1. WHILE an operation is in progress THEN the System SHALL display a loading indicator on the relevant button
2. WHILE an operation is in progress THEN the System SHALL disable the submit button to prevent duplicate submissions
3. WHEN an operation completes successfully THEN the System SHALL display a brief success message
4. WHEN an operation fails THEN the System SHALL display an error message that explains the failure

### Requirement 5

**User Story:** As a user, I want the create table form to support all partition strategies, so that I can configure tables according to my data distribution needs.

#### Acceptance Criteria

1. WHEN configuring partition strategy THEN the System SHALL provide options for PKHash, Hash, Range, DatetimeRange, Custom, and None strategies
2. WHEN PKHash or Hash strategy is selected THEN the System SHALL display a field for partition count
3. WHEN Hash strategy is selected THEN the System SHALL display a field selector for the partition field
4. WHEN Range strategy is selected THEN the System SHALL display fields for start value, step, and partition field
5. WHEN DatetimeRange strategy is selected THEN the System SHALL display fields for time granularity, timezone, and partition field
