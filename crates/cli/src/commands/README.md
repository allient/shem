# Shem CLI Commands

This directory contains the implementation of Shem's command-line interface commands.

## Introspect Command Implementation Status

The `introspect` command extracts PostgreSQL database schema and generates SQL files using the new unified model architecture. Below is a comprehensive table showing which PostgreSQL objects are implemented and which are missing.

### PostgreSQL Objects Implementation Status (Unified Architecture)

| Object Type | Unified Model | Schema Field | Introspection | SQL Generation | Status |
|-------------|---------------|--------------|---------------|----------------|---------|
| **Tables** | `Relation::Table` | `relations` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Views** | `Relation::View` | `relations` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Materialized Views** | `Relation::MaterializedView` | `relations` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Foreign Tables** | `Relation::ForeignTable` | `relations` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Functions** | `Routine::Function` | `routines` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Procedures** | `Routine::Procedure` | `routines` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Aggregates** | `Routine::Aggregate` | `routines` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Enums** | `Type::Enum` | `types` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Composite Types** | `Type::Composite` | `types` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Range Types** | `Type::Range` | `types` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Domains** | `Type::Domain` | `types` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Base Types** | `Type::Base` | `types` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Pseudo Types** | `Type::Pseudo` | `types` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Sequences** | `Sequence` | `sequences` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Extensions** | `Extension` | `extensions` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Triggers** | `Trigger` | Embedded in Relations | ✅ Implemented | ✅ Implemented | **Complete** |
| **Event Triggers** | `EventTrigger` | `event_triggers` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Policies** | `Policy` | `policies` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Rules** | `Rule` | `rules` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Named Schemas** | `Schema` | `schemas` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Servers** | `Server` | `servers` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Publications** | `Publication` | `publications` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Subscriptions** | `Subscription` | `subscriptions` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Roles** | `Role` | `roles` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Tablespaces** | `Tablespace` | `tablespaces` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Foreign Data Wrappers** | `ForeignDataWrapper` | `foreign_data_wrappers` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Collations** | `Collation` | `collations` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Conversions** | `Conversion` | `conversions` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Operators** | `Operator` | `operators` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Operator Classes** | `OpClass` | `op_classes` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Operator Families** | `OpFamily` | `op_families` | ✅ Implemented | ✅ Implemented | **Complete** |
| **Indexes** | Embedded in Relations | Embedded | ✅ Implemented | ✅ Implemented | **Complete** |
| **Foreign Key Constraints** | Embedded in Relations | Embedded | ✅ Implemented | ✅ Implemented | **Complete** |
| **Array Types** | Property of Base Types | Embedded | ✅ Implemented | ✅ Implemented | **Complete** |
| **Multirange Types** | Property of Range Types | Embedded | ✅ Implemented | ✅ Implemented | **Complete** |

### Legend

- ✅ **Implemented**: Feature is fully implemented and working
- ❌ **Missing**: Feature is not implemented
- **Complete**: Object is fully supported (introspection + SQL generation)
- **Partial**: Object is introspected but not included in SQL generation

### Summary

- **32 objects** are fully implemented (Complete)
- **0 objects** are partially implemented (Partial)
- **Total**: 32 PostgreSQL object types supported in the unified model architecture

### New Unified Architecture Benefits

The new unified model architecture provides several advantages:

1. **Type Safety**: Compile-time guarantees for object types through enum variants
2. **Unified APIs**: Single methods handle multiple object variants (e.g., `create_relation` handles tables, views, materialized views)
3. **Extensibility**: Easy to add new object types by extending the enums
4. **Consistency**: Uniform handling across the codebase
5. **Performance**: Efficient batch processing and reduced code duplication

### Key Architectural Changes

#### 1. Unified Object Models

Instead of separate structs for each object type, the new architecture uses unified enums:

```rust
// Old approach: Separate structs
pub struct Table { /* ... */ }
pub struct View { /* ... */ }
pub struct MaterializedView { /* ... */ }

// New approach: Unified enum
pub enum Relation {
    Table(Table),
    View(View),
    MaterializedView(MaterializedView),
    ForeignTable(ForeignTable),
}

pub enum Type {
    Base(BaseType),
    Composite(CompositeType),
    Domain(Domain),
    Enum(EnumType),
    Range(RangeType),
    Pseudo(PseudoType),
}

pub enum Routine {
    Function(Function),
    Procedure(Procedure),
    Aggregate(Aggregate),
}
```

#### 2. Organized Database Model

The `DatabaseModel` struct organizes objects by scope:

```rust
pub struct DatabaseModel {
    // Global Objects (Cluster-wide)
    pub roles: HashMap<String, Role>,
    pub tablespaces: HashMap<String, Tablespace>,
    
    // Database-Scoped Objects
    pub schemas: HashMap<String, Schema>,
    pub extensions: HashMap<String, Extension>,
    pub publications: HashMap<String, Publication>,
    pub subscriptions: HashMap<String, Subscription>,
    pub foreign_data_wrappers: HashMap<String, ForeignDataWrapper>,
    pub servers: HashMap<String, Server>,
    pub event_triggers: HashMap<String, EventTrigger>,
    
    // Schema-Scoped Objects
    pub relations: HashMap<String, Relation>,
    pub sequences: HashMap<String, Sequence>,
    pub types: HashMap<String, Type>,
    pub routines: HashMap<String, Routine>,
    pub collations: HashMap<String, Collation>,
    pub conversions: HashMap<String, Conversion>,
    pub policies: HashMap<String, Policy>,
    pub rules: HashMap<String, Rule>,
    pub operators: HashMap<String, Operator>,
    pub op_classes: HashMap<String, OpClass>,
    pub op_families: HashMap<String, OpFamily>,
    
    // Relationship Objects
    pub publication_tables: HashMap<String, PublicationTable>,
}
```

#### 3. Unified SQL Generator

The SQL generator now uses unified methods:

```rust
impl SqlGenerator for PostgresSqlGenerator {
    // Unified object generators
    fn create_relation(&self, relation: &Relation) -> Result<String>;
    fn create_type(&self, t: &Type) -> Result<String>;
    fn create_routine(&self, routine: &Routine) -> Result<String>;
    
    // Individual object generators
    fn create_schema(&self, schema: &Schema) -> Result<String>;
    fn create_sequence(&self, seq: &Sequence) -> Result<String>;
    // ... more generators
}
```

### Implementation Status by Category

#### ✅ Fully Implemented Categories

1. **Data Structures** (4/4): Tables, Views, Materialized Views, Foreign Tables
2. **Data Types** (6/6): Enums, Composite Types, Domains, Range Types, Base Types, Pseudo Types
3. **Logic & Functions** (3/3): Functions, Procedures, Aggregates
4. **Security** (2/2): Policies, Roles
5. **Storage** (3/3): Sequences, Indexes, Tablespaces
6. **Extensions & Replication** (4/4): Extensions, Publications, Subscriptions, Foreign Data Wrappers
7. **Infrastructure** (6/6): Schemas, Servers, Collations, Conversions, Operators, Operator Classes, Operator Families
8. **Triggers & Rules** (3/3): Triggers, Event Triggers, Rules

#### 🔶 Partially Implemented Categories

None - all categories are now fully implemented.

### Files to Update

The new architecture is already fully implemented in the pg crate. The CLI commands README has been updated to reflect the current status.

### Testing

Each object type has corresponding tests in:
- `crates/pg/tests/sql_generator/` - SQL generation tests
- `crates/pg/tests/introspection/` - Introspection tests
- `crates/cli/tests/introspect/` - Integration tests for the introspect command

### Migration from Old Architecture

The transition to the unified architecture is complete. All existing functionality has been preserved while providing:

1. **Better type safety** through enum variants
2. **Reduced code duplication** through unified methods
3. **Improved maintainability** through organized structure
4. **Enhanced performance** through batch processing
5. **Future extensibility** through the unified model approach 