# Implementation Plan

- [x] 1. Add CSS styles for new components
  - Add modal overlay and modal container styles
  - Add form group, input, select, and button styles
  - Add notification/toast styles for success/error messages
  - Add language selector styles for console
  - _Requirements: 1.1, 1.5, 2.1, 3.1, 4.1, 4.2_

- [ ] 2. Implement Create Table Modal
  - [x] 2.1 Add modal HTML structure and Vue data bindings
    - Create modal overlay with form fields for table name, description, primary key
    - Add field definition section with add/remove field buttons
    - Add partition strategy selector with conditional fields
    - _Requirements: 1.1, 1.5, 5.1, 5.2, 5.3, 5.4, 5.5_
  - [x] 2.2 Implement buildCreateTableMutation function
    - Build GraphQL mutation string based on form data
    - Handle all partition strategy types (PKHash, Hash, Range, DatetimeRange, None)
    - Properly escape string values
    - _Requirements: 1.2_
  - [-] 2.3 Write property test for mutation generation
    - **Property 3: CreateTable Mutation Generation**
    - **Validates: Requirements 1.2**
  - [x] 2.4 Implement createTable method
    - Validate form inputs before submission
    - Call buildCreateTableMutation and execute GraphQL
    - Handle success: close modal, refresh table list, show notification
    - Handle error: display error message, keep modal open
    - _Requirements: 1.2, 1.3, 1.4, 4.1, 4.2, 4.3, 4.4_

- [ ] 3. Implement Delete Table Functionality
  - [ ] 3.1 Add delete button to table items and confirmation modal
    - Add delete icon button to each table item
    - Create confirmation modal with warning message
    - _Requirements: 2.1_
  - [ ] 3.2 Implement deleteTable method
    - Send dropTable mutation to GraphQL endpoint
    - Handle success: close dialog, refresh list, clear selection if needed
    - Handle error: display error message
    - _Requirements: 2.2, 2.3, 2.4, 2.5, 4.3, 4.4_

- [ ] 4. Implement Dual-Language Console
  - [ ] 4.1 Add language selector UI to console header
    - Add SQL/GraphQL toggle buttons or dropdown
    - Store selected language in Vue data
    - _Requirements: 3.1_
  - [ ] 4.2 Implement wrapSqlInGraphQL function
    - Escape SQL string for GraphQL
    - Wrap in query { query(sql: "...") { columns rows } } format
    - _Requirements: 3.2_
  - [ ]* 4.3 Write property test for SQL wrapping
    - **Property 1: SQL to GraphQL Wrapping Correctness**
    - **Validates: Requirements 3.2**
  - [ ]* 4.4 Write property test for GraphQL passthrough
    - **Property 2: GraphQL Passthrough Integrity**
    - **Validates: Requirements 3.3**
  - [ ] 4.5 Update executeQuery method to handle both languages
    - Check selected language mode
    - If SQL: wrap query using wrapSqlInGraphQL
    - If GraphQL: send query directly
    - _Requirements: 3.2, 3.3, 3.4, 3.5_

- [ ] 5. Add Notification System
  - [ ] 5.1 Implement toast notification component
    - Add notification container and styles
    - Support success and error message types
    - Auto-dismiss after timeout
    - _Requirements: 4.3, 4.4_
  - [ ] 5.2 Integrate notifications with all operations
    - Show success notification after table creation
    - Show success notification after table deletion
    - Show error notifications on failures
    - _Requirements: 1.3, 2.3, 4.3, 4.4_

- [ ] 6. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 7. Final Integration and Polish
  - [ ] 7.1 Test complete workflow
    - Test create table with all partition strategies
    - Test delete table with confirmation
    - Test SQL and GraphQL console modes
    - _Requirements: All_
  - [ ] 7.2 Fix any remaining issues
    - Address edge cases discovered during testing
    - Ensure proper error handling throughout
    - _Requirements: All_

- [ ] 8. Final Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.
