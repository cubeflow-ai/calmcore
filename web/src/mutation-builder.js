/**
 * Mutation builder functions for CalmCore GraphQL API
 * Extracted for testability
 */

/**
 * Escape a string for use in GraphQL
 * @param {string} str - The string to escape
 * @returns {string} - The escaped string
 */
export function escapeGraphQLString(str) {
    if (str === null || str === undefined) return '';
    return String(str)
        .replace(/\\/g, '\\\\')
        .replace(/"/g, '\\"')
        .replace(/\n/g, '\\n')
        .replace(/\r/g, '\\r')
        .replace(/\t/g, '\\t');
}

/**
 * Build GraphQL mutation string for creating a table
 * Handles all partition strategy types (PKHash, Hash, Range, DatetimeRange, Custom, None)
 * @param {Object} tableConfig - The table configuration object
 * @returns {string} - The GraphQL mutation string
 */
export function buildCreateTableMutation(tableConfig) {
    const escapedName = escapeGraphQLString(tableConfig.name);
    const escapedDescription = tableConfig.description
        ? `description: "${escapeGraphQLString(tableConfig.description)}"`
        : '';
    // Note: GraphQL uses camelCase (primaryKey) not snake_case (primary_key)
    const escapedPrimaryKey = tableConfig.primaryKey
        ? `primaryKey: "${escapeGraphQLString(tableConfig.primaryKey)}"`
        : '';
    const storeSource = tableConfig.storeSource !== undefined
        ? `storeSource: ${tableConfig.storeSource}`
        : '';

    // Build partition strategy
    let partitionStrategyStr = '';
    const ps = tableConfig.partitionStrategy;

    if (ps && ps.type !== 'NONE') {
        switch (ps.type) {
            case 'PKHASH':
                partitionStrategyStr = `partitionStrategy: {
                    pkHash: { numPartitions: ${ps.pkHashPartitions || 4} }
                }`;
                break;
            case 'HASH':
                partitionStrategyStr = `partitionStrategy: {
                    hash: {
                        field: "${escapeGraphQLString(ps.hashField)}"
                        numPartitions: ${ps.hashPartitions || 4}
                    }
                }`;
                break;
            case 'RANGE':
                partitionStrategyStr = `partitionStrategy: {
                    range: {
                        field: "${escapeGraphQLString(ps.rangeField)}"
                        start: ${ps.rangeStart || 0}
                        step: ${ps.rangeStep || 1000000}
                        numPartitions: ${ps.rangePartitions || 10}
                        parallelism: ${ps.rangeParallelism || 1}
                    }
                }`;
                break;
            case 'DATETIME_RANGE':
                partitionStrategyStr = `partitionStrategy: {
                    datetimeRange: {
                        field: "${escapeGraphQLString(ps.datetimeField)}"
                        granularity: ${ps.datetimeGranularity || 'DAY'}
                        timezone: "${escapeGraphQLString(ps.datetimeTimezone || 'UTC')}"
                        parallelism: ${ps.datetimeParallelism || 1}
                    }
                }`;
                break;
            case 'CUSTOM':
                partitionStrategyStr = `partitionStrategy: {
                    custom: { expression: "custom" }
                }`;
                break;
        }
    } else {
        // None partition strategy
        partitionStrategyStr = `partitionStrategy: {
            none: { enabled: true }
        }`;
    }

    // Build fields array
    // Note: GraphQL uses camelCase (fieldType) not snake_case (field_type)
    const fieldsStr = tableConfig.fields
        .filter(f => f.name && f.name.trim())
        .map(field => {
            const parts = [
                `name: "${escapeGraphQLString(field.name)}"`,
                `fieldType: ${field.type || 'KEYWORD'}`
            ];

            if (field.indexed !== undefined) {
                parts.push(`indexed: ${field.indexed}`);
            }
            if (field.nullable !== undefined) {
                parts.push(`nullable: ${field.nullable}`);
            }
            if (field.description) {
                parts.push(`description: "${escapeGraphQLString(field.description)}"`);
            }

            return `{ ${parts.join(', ')} }`;
        })
        .join('\n                        ');

    // Build the complete mutation
    const inputParts = [
        `name: "${escapedName}"`
    ];

    if (escapedDescription) inputParts.push(escapedDescription);
    if (escapedPrimaryKey) inputParts.push(escapedPrimaryKey);
    if (storeSource) inputParts.push(storeSource);
    if (partitionStrategyStr) inputParts.push(partitionStrategyStr);
    inputParts.push(`fields: [
                        ${fieldsStr}
                    ]`);

    const mutation = `mutation {
                    createTable(input: {
                        ${inputParts.join('\n                        ')}
                    }) {
                        name
                        partitionCount
                        fields {
                            name
                            fieldType
                            indexed
                        }
                    }
                }`;

    return mutation;
}

/**
 * Build GraphQL mutation string for dropping (deleting) a table
 * @param {string} tableName - The name of the table to drop
 * @returns {string} - The GraphQL mutation string
 */
export function buildDropTableMutation(tableName) {
    const escapedName = escapeGraphQLString(tableName);
    return `mutation {
                            dropTable(name: "${escapedName}")
                        }`;
}

/**
 * Wrap a SQL query in a GraphQL query structure
 * Escapes the SQL string for GraphQL and wraps it in the query format
 * @param {string} sql - The SQL query string to wrap
 * @returns {string} - The GraphQL query string containing the SQL
 */
export function wrapSqlInGraphQL(sql) {
    const escapedSql = escapeGraphQLString(sql);
    return `query { query(sql: "${escapedSql}") { columns rows } }`;
}
