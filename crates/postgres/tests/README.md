cargo test -p shem-postgres -- --list
cargo test -p shem-postgres --test sql_generator
cargo test -p shem-postgres --test sql_generator -- --nocapture
cargo test test_generate_create_table -- --nocapture