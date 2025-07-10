# Shem CLI Commands

This directory contains the implementation of Shem's command-line interface commands.

## Introspect Command Implementation Status

The `introspect` command extracts PostgreSQL database schema and generates SQL files. Below is a comprehensive table showing which PostgreSQL objects are implemented and which are missing.

### PostgreSQL Objects Implementation Status

| Object Type | Schema Field | SchemaObject Enum | Introspection | SQL Generation | Status |
|-------------|--------------|-------------------|---------------|----------------|---------|
| **Extensions** | `extensions` | ✅ `Extension` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Collations** | `collations` | ✅ `Collation` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Enums** | `enums` | ✅ `Enum` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Composite Types** | `composite_types` | ✅ `CompositeType` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Range Types** | `range_types` | ✅ `RangeType` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Domains** | `domains` | ✅ `Domain` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Sequences** | `sequences` | ✅ `Sequence` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Tables** | `tables` | ✅ `Table` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Views** | `views` | ✅ `View` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Materialized Views** | `materialized_views` | ✅ `MaterializedView` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Functions** | `functions` | ✅ `Function` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Procedures** | `procedures` | ✅ `Procedure` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Triggers** | `triggers` | ✅ `Trigger` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Constraint Triggers** | `constraint_triggers` | ✅ `ConstraintTrigger` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Event Triggers** | `event_triggers` | ✅ `EventTrigger` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Policies** | `policies` | ✅ `Policy` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Rules** | `rules` | ✅ `Rule` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Named Schemas** | `named_schemas` | ❌ Missing | ✅ Implemented | ❌ Missing | **Partial** |
| **Servers** | `servers` | ❌ Missing | ✅ Implemented | ❌ Missing | **Partial** |
| **Publications** | `publications` | ❌ Missing | ✅ Implemented | ❌ Missing | **Partial** |
| **Subscriptions** | `subscriptions` | ❌ Missing | ✅ Implemented | ❌ Missing | **Partial** |
| **Roles** | `roles` | ❌ Missing | ✅ Implemented | ❌ Missing | **Partial** |
| **Tablespaces** | `tablespaces` | ❌ Missing | ✅ Implemented | ❌ Missing | **Partial** |
| **Foreign Tables** | `foreign_tables` | ❌ Missing | ✅ Implemented | ❌ Missing | **Partial** |
| **Foreign Data Wrappers** | `foreign_data_wrappers` | ❌ Missing | ✅ Implemented | ❌ Missing | **Partial** |
| **Foreign Key Constraints** | `foreign_key_constraints` | ❌ Missing | ✅ Implemented | ❌ Missing | **Partial** |
| **Base Types** | `base_types` | ❌ Missing | ✅ Implemented | ❌ Missing | **Partial** |
| **Array Types** | `array_types` | ❌ Missing | ✅ Implemented | ❌ Missing | **Partial** |
| **Multirange Types** | `multirange_types` | ❌ Missing | ✅ Implemented | ❌ Missing | **Partial** |

### Legend

- ✅ **Implemented**: Feature is fully implemented and working
- ❌ **Missing**: Feature is not implemented
- **Complete**: Object is fully supported (introspection + SQL generation)
- **Partial**: Object is introspected but not included in SQL generation

### Summary

- **17 objects** are fully implemented (Complete)
- **12 objects** are partially implemented (Partial - introspected but not serialized)
- **Total**: 29 PostgreSQL object types supported in the Schema struct

### Impact of Missing Objects

Objects marked as "Partial" are introspected from the database but are **not included in the generated SQL file**. This means:

1. **Incomplete schema dumps** - These objects won't be recreated when the SQL is executed
2. **Missing dependencies** - Some objects might depend on the missing ones
3. **Inconsistent behavior** - Objects are read but not written

### Recommendations

To achieve complete schema serialization, the following should be implemented:

1. **Add missing variants to `SchemaObject` enum**:
   ```rust
   enum SchemaObject<'a> {
       // ... existing variants ...
       NamedSchema(&'a NamedSchema),
       Server(&'a Server),
       Publication(&'a Publication),
       Subscription(&'a Subscription),
       Role(&'a Role),
       Tablespace(&'a Tablespace),
       ForeignTable(&'a ForeignTable),
       ForeignDataWrapper(&'a ForeignDataWrapper),
       ForeignKeyConstraint(&'a ForeignKeyConstraint),
       BaseType(&'a BaseType),
       ArrayType(&'a ArrayType),
       MultirangeType(&'a MultirangeType),
   }
   ```

2. **Implement SQL generation functions** for each missing object type
3. **Add dependency resolution logic** for the new object types
4. **Update the dependency ordering** in `resolve_schema_dependencies()`

### Files to Update

- `crates/cli/src/commands/introspect.rs` - Add missing SchemaObject variants and SQL generation
- `crates/core/src/schema.rs` - Schema struct is already complete
- `crates/postgres/src/introspection.rs` - Introspection is already complete

### Testing

Each new object type should have corresponding tests in:
- `crates/cli/tests/introspect/` - Integration tests for the introspect command
- `crates/postgres/tests/` - Unit tests for introspection functions 