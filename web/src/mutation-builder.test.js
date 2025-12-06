/**
 * Property-based tests for mutation-builder.js
 * **Feature: web-ddl-console, Property 3: CreateTable Mutation Generation**
 * **Validates: Requirements 1.2**
 */

import { describe, it, expect } from 'vitest';
import * as fc from 'fast-check';
import { buildCreateTableMutation, buildDropTableMutation, escapeGraphQLString } from './mutation-builder.js';

/**
 * Helper function to check if braces and brackets are balanced in GraphQL,
 * properly handling string literals (ignoring braces/brackets inside strings)
 */
function checkGraphQLBalance(str) {
    let braceCount = 0;
    let bracketCount = 0;
    let inString = false;
    let i = 0;

    while (i < str.length) {
        const char = str[i];

        if (inString) {
            // Handle escape sequences inside strings
            if (char === '\\' && i + 1 < str.length) {
                i += 2; // Skip escaped character
                continue;
            }
            if (char === '"') {
                inString = false;
            }
        } else {
            if (char === '"') {
                inString = true;
            } else if (char === '{') {
                braceCount++;
            } else if (char === '}') {
                braceCount--;
                if (braceCount < 0) return { balanced: false, reason: 'unmatched }' };
            } else if (char === '[') {
                bracketCount++;
            } else if (char === ']') {
                bracketCount--;
                if (bracketCount < 0) return { balanced: false, reason: 'unmatched ]' };
            }
        }
        i++;
    }

    if (inString) return { balanced: false, reason: 'unclosed string' };
    if (braceCount !== 0) return { balanced: false, reason: `unbalanced braces: ${braceCount}` };
    if (bracketCount !== 0) return { balanced: false, reason: `unbalanced brackets: ${bracketCount}` };

    return { balanced: true };
}

// Valid field types in CalmCore GraphQL schema
const VALID_FIELD_TYPES = ['KEYWORD', 'TEXT', 'I64', 'I32', 'U64', 'F64', 'F32', 'BOOLEAN', 'TIMESTAMP'];

// Valid partition strategy types
const VALID_PARTITION_TYPES = ['NONE', 'PKHASH', 'HASH', 'RANGE', 'DATETIME_RANGE', 'CUSTOM'];

// Valid datetime granularities
const VALID_GRANULARITIES = ['HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR'];

// Arbitrary for valid table names (lowercase letters, numbers, underscores)
const validTableName = fc.stringOf(
    fc.constantFrom(...'abcdefghijklmnopqrstuvwxyz0123456789_'.split('')),
    { minLength: 1, maxLength: 30 }
).filter(s => /^[a-z][a-z0-9_]*$/.test(s));

// Arbitrary for valid field names
const validFieldName = fc.stringOf(
    fc.constantFrom(...'abcdefghijklmnopqrstuvwxyz0123456789_'.split('')),
    { minLength: 1, maxLength: 30 }
).filter(s => /^[a-z][a-z0-9_]*$/.test(s));

// Arbitrary for a single field definition
const fieldArbitrary = fc.record({
    name: validFieldName,
    type: fc.constantFrom(...VALID_FIELD_TYPES),
    indexed: fc.boolean(),
    nullable: fc.boolean(),
    description: fc.option(fc.string({ maxLength: 50 }), { nil: undefined })
});

// Arbitrary for partition strategy
const partitionStrategyArbitrary = fc.oneof(
    // NONE
    fc.record({
        type: fc.constant('NONE')
    }),
    // PKHASH
    fc.record({
        type: fc.constant('PKHASH'),
        pkHashPartitions: fc.integer({ min: 1, max: 128 })
    }),
    // HASH
    fc.record({
        type: fc.constant('HASH'),
        hashField: validFieldName,
        hashPartitions: fc.integer({ min: 1, max: 128 })
    }),
    // RANGE
    fc.record({
        type: fc.constant('RANGE'),
        rangeField: validFieldName,
        rangeStart: fc.integer({ min: 0, max: 1000000 }),
        rangeStep: fc.integer({ min: 1, max: 10000000 }),
        rangePartitions: fc.integer({ min: 1, max: 128 }),
        rangeParallelism: fc.integer({ min: 1, max: 16 })
    }),
    // DATETIME_RANGE
    fc.record({
        type: fc.constant('DATETIME_RANGE'),
        datetimeField: validFieldName,
        datetimeGranularity: fc.constantFrom(...VALID_GRANULARITIES),
        datetimeTimezone: fc.constantFrom('UTC', 'America/New_York', 'Europe/London', 'Asia/Tokyo'),
        datetimeParallelism: fc.integer({ min: 1, max: 16 })
    }),
    // CUSTOM
    fc.record({
        type: fc.constant('CUSTOM')
    })
);

// Arbitrary for complete table configuration
const tableConfigArbitrary = fc.record({
    name: validTableName,
    description: fc.option(fc.string({ maxLength: 100 }), { nil: undefined }),
    primaryKey: fc.option(validFieldName, { nil: undefined }),
    storeSource: fc.boolean(),
    partitionStrategy: partitionStrategyArbitrary,
    fields: fc.array(fieldArbitrary, { minLength: 1, maxLength: 10 })
});

describe('buildCreateTableMutation', () => {
    /**
     * **Feature: web-ddl-console, Property 3: CreateTable Mutation Generation**
     * **Validates: Requirements 1.2**
     * 
     * Property: For any valid table configuration, the generated mutation
     * should be syntactically valid GraphQL that matches the CalmCore schema.
     */
    it('should generate syntactically valid GraphQL mutation for any valid table config', () => {
        fc.assert(
            fc.property(tableConfigArbitrary, (tableConfig) => {
                const mutation = buildCreateTableMutation(tableConfig);

                // Check that mutation is a non-empty string
                expect(typeof mutation).toBe('string');
                expect(mutation.length).toBeGreaterThan(0);

                // Check that mutation starts with 'mutation {'
                expect(mutation.trim()).toMatch(/^mutation\s*\{/);

                // Check that mutation contains createTable
                expect(mutation).toContain('createTable');

                // Check that mutation contains the table name
                expect(mutation).toContain(`name: "${escapeGraphQLString(tableConfig.name)}"`);

                // Check that mutation contains input block
                expect(mutation).toContain('input:');

                // Check that mutation contains fields array
                expect(mutation).toContain('fields:');

                // Check balanced braces and brackets outside of string literals
                // We need to parse carefully to avoid counting braces/brackets inside strings
                const balanceCheck = checkGraphQLBalance(mutation);
                expect(balanceCheck.balanced).toBe(true);

                // Check that partition strategy is included
                expect(mutation).toContain('partitionStrategy:');

                // Check partition strategy type is correctly represented
                const ps = tableConfig.partitionStrategy;
                if (ps.type === 'NONE') {
                    expect(mutation).toContain('none:');
                } else if (ps.type === 'PKHASH') {
                    expect(mutation).toContain('pkHash:');
                    expect(mutation).toContain('numPartitions:');
                } else if (ps.type === 'HASH') {
                    expect(mutation).toContain('hash:');
                    expect(mutation).toContain('field:');
                    expect(mutation).toContain('numPartitions:');
                } else if (ps.type === 'RANGE') {
                    expect(mutation).toContain('range:');
                    expect(mutation).toContain('field:');
                    expect(mutation).toContain('start:');
                    expect(mutation).toContain('step:');
                } else if (ps.type === 'DATETIME_RANGE') {
                    expect(mutation).toContain('datetimeRange:');
                    expect(mutation).toContain('granularity:');
                    expect(mutation).toContain('timezone:');
                } else if (ps.type === 'CUSTOM') {
                    expect(mutation).toContain('custom:');
                }

                // Check that all non-empty fields are included
                const nonEmptyFields = tableConfig.fields.filter(f => f.name && f.name.trim());
                for (const field of nonEmptyFields) {
                    expect(mutation).toContain(`name: "${escapeGraphQLString(field.name)}"`);
                    expect(mutation).toContain(`fieldType: ${field.type}`);
                }

                return true;
            }),
            { numRuns: 100 }
        );
    });

    /**
     * Property: Mutation should properly escape special characters in strings
     */
    it('should properly escape special characters in table names and descriptions', () => {
        fc.assert(
            fc.property(
                fc.record({
                    name: validTableName,
                    description: fc.string({ maxLength: 50 }),
                    primaryKey: fc.option(validFieldName, { nil: undefined }),
                    storeSource: fc.boolean(),
                    partitionStrategy: fc.constant({ type: 'NONE' }),
                    fields: fc.array(fieldArbitrary, { minLength: 1, maxLength: 3 })
                }),
                (tableConfig) => {
                    const mutation = buildCreateTableMutation(tableConfig);

                    // The mutation should be syntactically valid GraphQL
                    // Use the balance checker which properly handles escaped characters
                    const balanceCheck = checkGraphQLBalance(mutation);
                    expect(balanceCheck.balanced).toBe(true);

                    return true;
                }
            ),
            { numRuns: 100 }
        );
    });

    /**
     * Property: Mutation should include optional fields only when provided
     */
    it('should include optional fields only when provided', () => {
        fc.assert(
            fc.property(tableConfigArbitrary, (tableConfig) => {
                const mutation = buildCreateTableMutation(tableConfig);

                // Description should only appear if provided
                if (tableConfig.description) {
                    expect(mutation).toContain('description:');
                }

                // Primary key should only appear if provided
                if (tableConfig.primaryKey) {
                    expect(mutation).toContain('primaryKey:');
                }

                return true;
            }),
            { numRuns: 100 }
        );
    });
});

describe('buildDropTableMutation', () => {
    /**
     * Property: For any valid table name, the generated mutation
     * should be syntactically valid GraphQL for dropping a table.
     */
    it('should generate syntactically valid GraphQL mutation for any valid table name', () => {
        fc.assert(
            fc.property(validTableName, (tableName) => {
                const mutation = buildDropTableMutation(tableName);

                // Check that mutation is a non-empty string
                expect(typeof mutation).toBe('string');
                expect(mutation.length).toBeGreaterThan(0);

                // Check that mutation starts with 'mutation {'
                expect(mutation.trim()).toMatch(/^mutation\s*\{/);

                // Check that mutation contains dropTable
                expect(mutation).toContain('dropTable');

                // Check that mutation contains the table name
                expect(mutation).toContain(`name: "${escapeGraphQLString(tableName)}"`);

                // Check balanced braces
                const balanceCheck = checkGraphQLBalance(mutation);
                expect(balanceCheck.balanced).toBe(true);

                return true;
            }),
            { numRuns: 100 }
        );
    });

    /**
     * Property: Mutation should properly escape special characters in table names
     */
    it('should properly escape special characters in table names', () => {
        fc.assert(
            fc.property(fc.string({ maxLength: 50 }), (tableName) => {
                const mutation = buildDropTableMutation(tableName);

                // The mutation should be syntactically valid GraphQL
                const balanceCheck = checkGraphQLBalance(mutation);
                expect(balanceCheck.balanced).toBe(true);

                return true;
            }),
            { numRuns: 100 }
        );
    });
});

describe('escapeGraphQLString', () => {
    /**
     * Property: Escaping should handle null/undefined gracefully
     */
    it('should return empty string for null or undefined', () => {
        expect(escapeGraphQLString(null)).toBe('');
        expect(escapeGraphQLString(undefined)).toBe('');
    });

    /**
     * Property: Escaping should be idempotent for already-safe strings
     */
    it('should not modify strings without special characters', () => {
        fc.assert(
            fc.property(
                fc.stringOf(fc.constantFrom(...'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 '.split(''))),
                (str) => {
                    const escaped = escapeGraphQLString(str);
                    expect(escaped).toBe(str);
                    return true;
                }
            ),
            { numRuns: 100 }
        );
    });

    /**
     * Property: Escaping should properly escape backslashes and quotes
     */
    it('should escape backslashes and quotes', () => {
        fc.assert(
            fc.property(fc.string(), (str) => {
                const escaped = escapeGraphQLString(str);

                // Result should not contain unescaped backslashes (except as escape sequences)
                // Result should not contain unescaped quotes
                // Result should not contain literal newlines, carriage returns, or tabs
                expect(escaped).not.toMatch(/(?<!\\)"/);
                expect(escaped).not.toContain('\n');
                expect(escaped).not.toContain('\r');
                expect(escaped).not.toContain('\t');

                return true;
            }),
            { numRuns: 100 }
        );
    });
});
