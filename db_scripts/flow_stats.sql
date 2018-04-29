SELECT ts.to_state, count(*)
FROM tmp_stats ts
GROUP BY ts.to_state;