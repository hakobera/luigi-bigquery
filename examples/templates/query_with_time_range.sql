SELECT
  STRFTIME_UTC_USEC(TIMESTAMP(repository_created_at), '%Y-%m') AS month,
  count(*) cnt
FROM
  [publicdata:samples.github_timeline]
WHERE
  TIMESTAMP(repository_created_at) >= '{{ task.year }}-01-01 00:00:00'
  AND
  TIMESTAMP(repository_created_at) <= '{{ task.year + 1 }}-01-01 00:00:00'
GROUP BY
  month
ORDER BY
  month
