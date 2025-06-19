-- Seed sample_data
INSERT INTO sample_data (
    small_val, int_val, big_val, decimal_val, numeric_val, real_val, double_val, money_val,
    char_val, varchar_val, text_val, citext_val, name_val, bool_val,
    date_val, time_val, timetz_val, timestamp_val, timestamptz_val, interval_val,
    bytea_val, uuid_val,
    inet_val, cidr_val, macaddr_val, macaddr8_val,
    json_val, jsonb_val, int_array,
    tsvector_val, tsquery_val, xml_val, bit_val, varbit_val, mood_val,
    point_val, line_val, lseg_val, box_val, path_val, polygon_val, circle_val,
    address, salary_range
)
VALUES (
    1, 100, 10000, 123.45, 123.4567, 1.23, 2.34, '$100.00',
    'abc', 'hello world', 'some text', 'CaseInsensitive', 'myname', TRUE,
    '2024-01-01', '10:00:00', '10:00:00+00', '2024-01-01 10:00:00', now(), '2 days',
    '\xDEADBEEF', uuid_generate_v4(),
    '192.168.1.1', '192.168.1.0/24', '08:00:2b:01:02:03', '08:00:2b:01:02:03:04:05',
    '{"a":1}', '{"b":2}', ARRAY[1,2,3],
    to_tsvector('simple', 'sample'), to_tsquery('simple', 'sample'), '<xml>data</xml>', B'1010', B'10101010', 'happy',
    '(1,1)', '{1,1,1}', '[(0,0),(1,1)]', '((0,0),(1,1))', '((0,0),(1,1),(2,2))', '((0,0),(1,0),(1,1),(0,1))', '<(0,0),1>',
    ROW('123 Main St', 'City', '12345'), '[100,200)'::salary_range
);

-- Seed related_data using inserted sample_data id
INSERT INTO related_data (sample_id, optional_sample_id, comment)
SELECT id, NULL, 'FK comment 1'
FROM sample_data
LIMIT 1;

INSERT INTO related_data (sample_id, optional_sample_id, comment)
SELECT id, id, 'Self-FK comment'
FROM sample_data
LIMIT 1;
