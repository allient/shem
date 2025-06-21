use serde::{Deserialize, Serialize};

/// Data type definitions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DataType {
    SmallInt,
    Integer,
    BigInt,
    Decimal(Option<u32>, Option<u32>),
    Numeric(Option<u32>, Option<u32>),
    Real,
    DoublePrecision,
    SmallSerial,
    Serial,
    BigSerial,
    Money,
    Character(Option<u32>),
    CharacterVarying(Option<u32>),
    Text,
    ByteA,
    Timestamp(Option<bool>),
    TimestampTz(Option<bool>),
    Date,
    Time(Option<bool>),
    TimeTz(Option<bool>),
    Interval(Option<IntervalField>),
    Boolean,
    Bit(Option<u32>),
    BitVarying(Option<u32>),
    Uuid,
    Json,
    JsonB,
    Xml,
    Array(Box<DataType>),
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum IntervalField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    YearToMonth,
    DayToHour,
    DayToMinute,
    DayToSecond,
    HourToMinute,
    HourToSecond,
    MinuteToSecond,
} 