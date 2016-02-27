CREATE TABLE test_perfview(id INT PRIMARY KEY, name VARCHAR(80), value DOUBLE, tm DATETIME);
CREATE VIEW perfview AS SELECT 'app.http.requests' AS dsname, 'COUNTER' as dstype, value, tm AS timestamp FROM test_perfview ORDER BY id;
INSERT INTO test_perfview VALUES(1, 'app.http.requests.in', 4.0, CURRENT_TIMESTAMP());
INSERT INTO test_perfview VALUES(2, 'app.http.requests.out', 10.0, CURRENT_TIMESTAMP());
